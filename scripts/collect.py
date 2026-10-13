"""분야별 언론사 RSS 수집 → 키워드 추출 → Cloudflare D1 저장 → 기간별 키워드 순위(snapshot) 생성

GitHub Actions(collect.yml)가 매시간 실행. 저장·조회는 모두 Worker API(issue-keyword) 경유.

환경변수
  API_BASE      Worker 주소 (기본 https://issue-keyword.bluebird1131.workers.dev)
  INGEST_TOKEN  Worker 수집 API 토큰 (Actions Secret)
  DAYS          다시 확인할 기간(일). 기본 2 = 어제 0시부터. 처음 채울 때만 7 등으로
  REKEY         1이면 기간 안 기사 키워드를 현재 설정(불용어·사전)으로 다시 계산해 덮어씀
  DRY           1이면 저장 없이 결과만 snapshot.local.json으로
"""
import hashlib, html, json, math, os, re, sys, time, collections
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import feedparser
import requests
from kiwipiepy import Kiwi

ROOT = Path(__file__).resolve().parent.parent
API = (os.environ.get("API_BASE") or "https://issue-keyword.bluebird1131.workers.dev").rstrip("/")
TOKEN = os.environ.get("INGEST_TOKEN", "")
DAYS = max(1, int(os.environ.get("DAYS") or 2))
REKEY = os.environ.get("REKEY") == "1"
DRY = os.environ.get("DRY") == "1"
KST = timezone(timedelta(hours=9))
NOW = datetime.now(KST)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36"

PERIODS = [("24h", "최근 24시간", None), ("3d", "3일", 3), ("7d", "7일", 7), ("30d", "30일", 30)]
TOP_N, RISE_N, TREND_DAYS, BASE_DAYS = 60, 40, 14, 14
DAILY_KEEP = 400  # 하루·분야별로 저장할 단어 수 (많이 나온 순)


def fmt(dt):
    return dt.astimezone(KST).strftime("%Y-%m-%dT%H:%M")


# ---------- 설정 ----------
FEEDS = {k: v for k, v in json.loads((ROOT / "config/feeds.json").read_text(encoding="utf-8")).items() if not k.startswith("_")}
CATS = list(FEEDS)


def words_file(name):
    out = []
    for line in (ROOT / "config" / name).read_text(encoding="utf-8").splitlines():
        out += line.split("#", 1)[0].split()
    return out


STOP = set(words_file("stopwords.txt"))
SYN = {k: v for k, v in json.loads((ROOT / "config/synonyms.json").read_text(encoding="utf-8")).items() if not k.startswith("_")}
kiwi = Kiwi()
for w in words_file("userdict.txt"):
    kiwi.add_user_word(w, "NNP", 0)


# ---------- 제목 정리·키워드 ----------
HEAD_RE = re.compile(r"\[[^\]]{1,20}\]|【[^】]{1,20}】|<[^>]{1,20}>|\([^)]*=[^)]*\)|\(종합\d*보?\)|\(\d+보\)|\(사진\)|\(영상\)")


def clean_title(t):
    t = html.unescape(re.sub(r"<[^>]+>", "", t or ""))
    return re.sub(r"\s+", " ", HEAD_RE.sub(" ", html.unescape(t))).strip()


def keywords(title):
    """제목 → 명사 키워드 집합. 띄어쓰기 없이 붙은 짧은 명사 두 개(전자+발찌)는 한 단어로 묶음"""
    toks = [t for t in kiwi.tokenize(title)]
    out, i = [], 0
    while i < len(toks):
        t = toks[i]
        if t.tag in ("NNP", "NNG", "SL", "SH"):
            form = t.form
            nxt = toks[i + 1] if i + 1 < len(toks) else None
            if (nxt and nxt.tag in ("NNP", "NNG") and nxt.start == t.start + t.len
                    and t.tag in ("NNP", "NNG") and len(form) <= 2 and len(nxt.form) <= 2
                    and not (i + 2 < len(toks) and toks[i + 2].tag in ("NNP", "NNG") and toks[i + 2].start == nxt.start + nxt.len)):
                form += nxt.form
                i += 1
            out.append(form)
        i += 1
    res = []
    for w in out:
        if re.fullmatch(r"[一-鿿]+", w) and w not in SYN:  # 한자 약칭(與野·美中)은 글자별로 바꿈
            out += [SYN[ch] for ch in w if SYN.get(ch)]
            continue
        w = SYN.get(w, w)
        if len(w) >= 2 and w not in STOP and not w.isdigit() and w not in res:
            res.append(w)
    return res


def norm_url(u):
    p = urlsplit((u or "").strip())
    q = [(k, v) for k, v in parse_qsl(p.query) if not k.lower().startswith(("utm_", "fbclid", "from", "ref"))]
    return urlunsplit((p.scheme, p.netloc.lower(), p.path, urlencode(q), ""))


def tkey(cat, title):
    return cat + "|" + re.sub(r"\W", "", title)


def art_id(cat, url):
    return hashlib.sha1(f"{cat}|{url}".encode()).hexdigest()[:16]


# ---------- RSS ----------
def fetch(job):
    cat, src, url = job
    st = {"cat": cat, "src": src, "url": url, "ok": False, "items": 0, "error": ""}
    for attempt in range(2):
        try:
            r = requests.get(url, headers={"user-agent": UA, "accept": "application/rss+xml,application/xml,text/xml,*/*"}, timeout=25)
            r.raise_for_status()
            f = feedparser.parse(r.content)
            if not f.entries:
                raise ValueError("항목 없음")
            items = []
            for e in f.entries:
                title = clean_title(e.get("title"))
                link = norm_url(e.get("link"))
                if not title or not link:
                    continue
                p = e.get("published_parsed") or e.get("updated_parsed")
                pub = datetime(*p[:6], tzinfo=timezone.utc) if p else NOW
                pub = min(pub, NOW)
                items.append({"cat": cat, "src": src, "title": title, "url": link, "pub": fmt(pub)})
            st.update(ok=True, items=len(items), error="")
            return st, items
        except Exception as ex:  # noqa: BLE001
            st["error"] = f"{type(ex).__name__}: {str(ex)[:120]}"
            time.sleep(2)
    return st, []


# ---------- Worker API ----------
S = requests.Session()
S.headers.update({"authorization": f"Bearer {TOKEN}", "user-agent": "issue-keyword-collector"})


def api(method, path, **kw):
    for attempt in range(3):
        r = S.request(method, API + path, timeout=90, **kw)
        if r.status_code < 500:
            break
        time.sleep(3 * (attempt + 1))
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} → HTTP {r.status_code}: {r.text[:300]}")
    return r


def load_recent(since):
    """기간 안 기사 전부 (Worker 1회 처리량 제한 때문에 1,000건씩)"""
    rows, after = [], ""
    while True:
        j = api("GET", "/api/recent", params={"since": since, "after": after, "limit": 1000}).json()
        rows += [dict(zip(j["cols"], r)) for r in j["rows"]]
        if not j.get("next"):
            return rows
        after = j["next"]


def load_daily(since_date):
    out = {}
    for line in api("GET", "/api/daily", params={"since": since_date}).text.splitlines():
        if "\t" in line:
            d, body = line.split("\t", 1)
            out[d] = json.loads(body)
    return out


# ---------- 집계 ----------
def count(arts):
    """[기사] → {분야: (기사 수, Counter 단어→기사 수, 단어→매체 집합)}"""
    res = {c: [0, collections.Counter(), collections.defaultdict(set)] for c in CATS}
    for a in arts:
        if a["cat"] not in res:
            continue
        r = res[a["cat"]]
        r[0] += 1
        for w in a["kws"].split():
            r[1][w] += 1
            r[2][w].add(a["src"])
    return res


def daily_body(arts):
    body = {"total": {}, "w": {}}
    for c, (n, cnt, srcs) in count(arts).items():
        body["total"][c] = n
        body["w"][c] = {w: [k, "|".join(sorted(srcs[w]))] for w, k in cnt.most_common(DAILY_KEEP)}
    return body


def merge_days(days):
    """일별 집계 여러 개 → {분야: (기사 수, Counter, 단어→매체 집합)}"""
    res = {c: [0, collections.Counter(), collections.defaultdict(set)] for c in CATS}
    for b in days:
        for c in CATS:
            res[c][0] += b["total"].get(c, 0)
            for w, (k, s) in b["w"].get(c, {}).items():
                res[c][1][w] += k
                res[c][2][w].update(s.split("|"))
    return res


def rank(cur, base, trend_of):
    """현재 기간 집계 + 비교 기간 집계 → (많이 다룬 순위, 급상승 순위)"""
    n_all, cnt, srcs = cur
    b_all, b_cnt = (base[0], base[1]) if base else (0, {})

    def item(w):
        k, m = cnt[w], len(srcs[w])
        it = {"w": w, "n": k, "m": m, "t": trend_of(w)}
        if b_all:
            exp = b_cnt.get(w, 0) / b_all * n_all
            it["r"] = None if b_cnt.get(w, 0) == 0 else round((k + 1) / (exp + 1), 1)
        return it

    cand = [w for w in cnt if len(srcs[w]) >= 2]
    top = sorted(cand, key=lambda w: (-(cnt[w] * (1 + len(srcs[w]) / 4)), w))[:TOP_N]
    rise = None
    if b_all:
        def z(w):
            exp = b_cnt.get(w, 0) / b_all * n_all
            return (cnt[w] - exp) / math.sqrt(exp + 1)
        rc = [w for w in cand if cnt[w] >= 3 and (cnt[w] + 1) / (b_cnt.get(w, 0) / b_all * n_all + 1) >= 2]
        rise = [item(w) for w in sorted(rc, key=lambda w: (-z(w), w))[:RISE_N]]
    return [item(w) for w in top], rise


def main():
    if not TOKEN and not DRY:
        sys.exit("INGEST_TOKEN 이 없습니다.")
    t0 = time.time()
    today = NOW.date()
    since_date = today - timedelta(days=DAYS - 1)
    since = f"{since_date}T00:00"

    jobs = [(c, s, u) for c, lst in FEEDS.items() for s, u in lst]
    with ThreadPoolExecutor(8) as ex:
        results = list(ex.map(fetch, jobs))
    status = [st for st, _ in results]
    fetched = [it for _, items in results for it in items if it["pub"] >= since]
    print(f"RSS {sum(s['ok'] for s in status)}/{len(status)}개 응답, 기간 안 항목 {len(fetched)}건")
    for s in status:
        if not s["ok"]:
            print(f"  ::warning::{s['cat']} {s['src']} 실패 — {s['error']}")

    existing = [] if DRY else load_recent(since)
    ids = {a["id"] for a in existing}
    tkeys = {tkey(a["cat"], a["title"]) for a in existing}
    new = []
    for it in fetched:
        it["id"], it["tkey"] = art_id(it["cat"], it["url"]), tkey(it["cat"], it["title"])
        if it["id"] in ids or it["tkey"] in tkeys:
            continue
        ids.add(it["id"]); tkeys.add(it["tkey"])
        it["kws"] = " ".join(keywords(it["title"]))
        new.append(it)
    if REKEY:
        for a in existing:
            a["kws"] = " ".join(keywords(a["title"]))
    print(f"기존 {len(existing)}건, 새 기사 {len(new)}건")

    if not DRY:
        for i in range(0, len(new), 100):
            api("POST", "/api/ingest", json={"articles": new[i:i + 100]})
        if REKEY:
            for i in range(0, len(existing), 500):
                api("POST", "/api/rekey", json={"articles": [{"id": a["id"], "kws": a["kws"]} for a in existing[i:i + 500]]})

    arts = existing + new
    # 일별 집계: 이번에 다시 본 날짜(since_date~오늘)만 새로 만들고, 그 전 날짜는 저장된 것 사용
    by_day = collections.defaultdict(list)
    for a in arts:
        by_day[a["pub"][:10]].append(a)
    fresh = {str(since_date + timedelta(days=i)): daily_body(by_day.get(str(since_date + timedelta(days=i)), [])) for i in range(DAYS)}
    stored = {} if DRY else load_daily(str(today - timedelta(days=30 + BASE_DAYS)))
    daily = {**stored, **fresh}
    if not DRY:
        api("PUT", "/api/daily", json={"rows": [{"date": d, "body": json.dumps(b, ensure_ascii=False, separators=(",", ":"))} for d, b in fresh.items()]})

    def day(i):
        return str(today - timedelta(days=i))

    trend_dates = [day(i) for i in range(TREND_DAYS - 1, -1, -1)]

    def trend_fn(cat):
        return lambda w: [daily.get(d, {}).get("w", {}).get(cat, {}).get(w, [0])[0] for d in trend_dates]

    out = {"generatedAt": fmt(NOW), "cats": CATS, "trendDates": trend_dates, "periods": [], "data": {}}
    for key, label, n_days in PERIODS:
        if n_days is None:
            frm = fmt(NOW - timedelta(hours=24))
            cur = count([a for a in arts if a["pub"] >= frm])
            base_days = [daily[day(i)] for i in range(2, 2 + BASE_DAYS) if day(i) in daily]
        else:
            frm = f"{day(n_days - 1)}T00:00"
            cur = merge_days([daily[day(i)] for i in range(n_days) if day(i) in daily])
            base_days = [daily[day(i)] for i in range(n_days, n_days + BASE_DAYS) if day(i) in daily] if n_days <= 7 else []
        base = merge_days(base_days) if len(base_days) >= 3 else None
        out["periods"].append({"key": key, "label": label, "from": frm, "baseDays": len(base_days), "rising": base is not None})
        out["data"][key] = {}
        for c in CATS:
            top, rise = rank(cur[c], base[c] if base else None, trend_fn(c))
            out["data"][key][c] = {"count": cur[c][0], "top": top, "rise": rise}

    srcs = collections.defaultdict(lambda: collections.Counter())
    for a in arts:
        if a["pub"] >= fmt(NOW - timedelta(hours=24)):
            srcs[a["src"]][a["cat"]] += 1
    run = {"at": fmt(NOW), "seconds": round(time.time() - t0), "fetched": len(fetched), "new": len(new),
           "feeds": status, "last24h": {s: dict(c) for s, c in srcs.items()}, "daysStored": len(daily)}
    out["run"] = run

    text = json.dumps(out, ensure_ascii=False, separators=(",", ":"))
    if DRY:
        (ROOT / "snapshot.local.json").write_text(text, encoding="utf-8")
    else:
        api("PUT", "/api/kv/snapshot", data=text.encode("utf-8"), headers={"content-type": "application/json"})
    print(f"키워드 순위 저장 ({len(text) // 1024}KB), {round(time.time() - t0)}초")


if __name__ == "__main__":
    main()
