"""분야별 언론사 RSS 수집 → 키워드 추출 → Cloudflare D1 저장 → 기간별 이슈·두 단어·단어 순위(snapshot) 생성

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
TOP_N, RISE_N, TREND_DAYS, BASE_DAYS, TREND_ITEMS = 50, 40, 14, 14, 30
DAILY_KEEP, PAIR_KEEP = 400, 300  # 하루·분야별로 저장할 단어·두 단어 묶음 수 (많이 나온 순)
LOAD_DAYS = 7          # 이슈 묶기·3일·7일 순위는 최근 7일 기사로 직접 계산 (30일은 일별 집계 합산)
CLUSTER_TH = 0.4       # 같은 이슈로 볼 공통 키워드 비율 (드문 단어 가중)
ISSUE_N, ISSUE_IDS = 20, 40


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
def fetch_ytn(cat, src, mcd):
    """YTN 은 RSS 가 없어 분야 목록의 '더 보기' JSON 을 4쪽(40건)까지 읽음"""
    items = []
    for page in range(1, 5):
        r = requests.post("https://www.ytn.co.kr/ajax/getMoreNews.php", data={"mcd": mcd, "hcd": "", "page": page, "pivot": ""},
                          headers={"user-agent": UA, "x-requested-with": "XMLHttpRequest", "referer": f"https://www.ytn.co.kr/news/list.php?mcd={mcd}"}, timeout=25)
        r.raise_for_status()
        for d in json.loads(r.text).get("data") or []:
            jk, title = str(d.get("join_key", "")), clean_title(d.get("title"))
            if not re.fullmatch(r"\d{12,}", jk) or not title:
                continue
            pub = datetime.strptime(jk[:12], "%Y%m%d%H%M").replace(tzinfo=KST)
            items.append({"cat": cat, "src": src, "title": title, "url": f"https://www.ytn.co.kr/_ln/{d.get('m_cd') or mcd}_{jk}", "pub": fmt(min(pub, NOW))})
    if not items:
        raise ValueError("항목 없음")
    return items


def fetch(job):
    cat, src, url = job
    st = {"cat": cat, "src": src, "url": url, "ok": False, "items": 0, "error": ""}
    for attempt in range(2):
        try:
            if url.startswith("ytn:"):
                items = fetch_ytn(cat, src, url[4:])
                st.update(ok=True, items=len(items), error="")
                return st, items
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
# 단어(w)와 두 단어 묶음(p)을 같은 방식으로 셈. 묶음 키는 가나다순 "a b", 화면 표시는 제목에 많이 나온 순서
PAIR_WORDS = 8  # 제목당 앞쪽 키워드 몇 개까지 짝을 지을지


def pair_keys(kws):
    ks = kws[:PAIR_WORDS]
    for i in range(len(ks)):
        for j in range(i + 1, len(ks)):
            a, b = ks[i], ks[j]
            yield " ".join(sorted((a, b))), f"{a} {b}"


def new_stat():
    return {"N": 0, "w": [collections.Counter(), collections.defaultdict(set), collections.defaultdict(set)],
            "p": [collections.Counter(), collections.defaultdict(set), collections.defaultdict(set)]}


def stats_from_arts(arts):
    """[기사] → {분야: {N, w: [기사 수, 매체 집합, 기사 번호 집합], p: 같은 구조}} + 묶음 표시 순서 투표"""
    res, order = {c: new_stat() for c in CATS}, collections.defaultdict(collections.Counter)
    for i, a in enumerate(arts):
        if a["cat"] not in res:
            continue
        r, kws = res[a["cat"]], a["kws"].split()
        r["N"] += 1
        for w in kws:
            r["w"][0][w] += 1; r["w"][1][w].add(a["src"]); r["w"][2][w].add(i)
        for k, disp in pair_keys(kws):
            r["p"][0][k] += 1; r["p"][1][k].add(a["src"]); r["p"][2][k].add(i)
            order[k][disp] += 1
    return res, order


def daily_body(arts):
    st, _ = stats_from_arts(arts)
    body = {"total": {}, "w": {}, "p": {}}
    for c, r in st.items():
        body["total"][c] = r["N"]
        body["w"][c] = {w: [k, "|".join(sorted(r["w"][1][w]))] for w, k in r["w"][0].most_common(DAILY_KEEP)}
        body["p"][c] = {w: [k, "|".join(sorted(r["p"][1][w]))] for w, k in r["p"][0].most_common(PAIR_KEEP) if k >= 2}
    return body


def merge_days(days):
    """일별 집계 여러 개 → stats_from_arts 와 같은 구조 (기사 번호 집합은 없음)"""
    res = {c: new_stat() for c in CATS}
    for b in days:
        for c in CATS:
            res[c]["N"] += b["total"].get(c, 0)
            for kind in ("w", "p"):
                for w, (k, s) in b.get(kind, {}).get(c, {}).items():
                    res[c][kind][0][w] += k
                    res[c][kind][1][w].update(s.split("|"))
    for c in CATS:
        for kind in ("w", "p"):
            res[c][kind][2] = None
    return res


def rank(n_all, kind_stat, base, trend_of, disp=lambda k: k, dedupe=False):
    """현재 집계 + 비교 기간 집계({N, cnt}) → (많이 다룬 순위, 급상승 순위). dedupe=같은 기사들을 가리키는 묶음은 하나만"""
    cnt, srcs, sets = kind_stat
    b_all, b_cnt = (base["N"], base["cnt"]) if base else (0, {})

    def ratio(k):
        exp = b_cnt.get(k, 0) / b_all * n_all
        return (cnt[k] + 1) / (exp + 1), (cnt[k] - exp) / math.sqrt(exp + 1)

    def item(k, i):
        it = {"w": disp(k), "n": cnt[k], "m": len(srcs[k])}
        if i < TREND_ITEMS:
            it["t"] = trend_of(k)
        if b_all:
            it["r"] = None if not b_cnt.get(k) else round(ratio(k)[0], 1)
        return it

    def pick(keys, limit):
        out, used, csets, adj = [], collections.Counter(), [], collections.defaultdict(set)
        for k in keys:
            if dedupe:
                ws = k.split()
                if any(used[w] >= 2 for w in ws):
                    continue
                if len(ws) == 2 and adj[ws[0]] & adj[ws[1]]:  # 홍라희-이재용, 이재용-증여 다음의 홍라희-증여 같은 삼각 중복
                    continue
                if sets is not None:
                    sk = sets[k]
                    if any(len(sk & c) >= 0.6 * len(sk) for c in csets):
                        continue
                    csets.append(sk)
                if len(ws) == 2:
                    adj[ws[0]].add(ws[1]); adj[ws[1]].add(ws[0])
                used.update(ws)
            out.append(k)
            if len(out) >= limit:
                break
        return out

    cand = [k for k in cnt if len(srcs[k]) >= 2 and cnt[k] >= 2]
    top = pick(sorted(cand, key=lambda k: (-(cnt[k] * (1 + len(srcs[k]) / 4)), k)), TOP_N)
    rise = None
    if b_all:
        rc = [k for k in cand if cnt[k] >= 3 and ratio(k)[0] >= 2]
        rise = [item(k, i) for i, k in enumerate(pick(sorted(rc, key=lambda k: (-ratio(k)[1], k)), RISE_N))]
    return [item(k, i) for i, k in enumerate(top)], rise


def issues(arts, idx, base, n_all):
    """같은 사건을 다룬 기사 묶기: 드문 단어(사람·사건명)에 가중치를 둔 공통 키워드 비율이 기준 이상이면 같은 이슈"""
    items = [i for i in idx if len(arts[i]["kws"].split()) >= 2]
    kset = {i: set(arts[i]["kws"].split()) for i in items}
    N = max(1, len(items))
    df = collections.Counter(w for i in items for w in kset[i])
    idf = {w: math.log(N / c) + 0.1 for w, c in df.items()}
    wsum = {i: sum(idf[w] for w in kset[i]) for i in items}
    par = {i: i for i in items}

    def find(x):
        while par[x] != x:
            par[x] = par[par[x]]
            x = par[x]
        return x

    bucket = collections.defaultdict(list)
    for i in items:
        for k, _ in pair_keys(arts[i]["kws"].split()):
            bucket[k].append(i)
    seen = set()
    for lst in bucket.values():
        if len(lst) > 400:  # 너무 흔한 짝은 후보에서 제외 (속도)
            continue
        for x in range(len(lst)):
            for y in range(x + 1, len(lst)):
                i, j = lst[x], lst[y]
                if (i, j) in seen:
                    continue
                seen.add((i, j))
                common = kset[i] & kset[j]
                if len(common) >= 2 and sum(idf[w] for w in common) / min(wsum[i], wsum[j]) >= CLUSTER_TH:
                    par[find(i)] = find(j)
    groups = collections.defaultdict(list)
    for i in items:
        groups[find(i)].append(i)

    b_all, b_cnt = (base["N"], base["cnt"]) if base else (0, {})
    out = []
    for g in groups.values():
        srcs = {arts[i]["src"] for i in g}
        if len(srcs) < 2:
            continue
        kc = collections.Counter(w for i in g for w in kset[i])
        top = [w for w, c in kc.most_common(4) if c >= 2][:3] or [w for w, _ in kc.most_common(2)]
        rep = max(g, key=lambda i: (sum(kc[w] for w in kset[i]) / (len(kset[i]) + 1), arts[i]["pub"]))
        g.sort(key=lambda i: arts[i]["pub"], reverse=True)
        it = {"t": arts[rep]["title"], "u": arts[rep]["url"], "s": arts[rep]["src"], "k": top, "n": len(g), "m": len(srcs),
              "last": arts[g[0]]["pub"], "ids": [arts[i]["id"] for i in g[:ISSUE_IDS]]}
        if b_all and len(top) >= 2:
            k = " ".join(sorted(top[:2]))
            exp = b_cnt.get(k, 0) / b_all * n_all
            it["r"] = None if not b_cnt.get(k) else round((len(g) + 1) / (exp + 1), 1)
        out.append(it)
    top = sorted(out, key=lambda x: (-x["m"] * math.log(x["n"] + 1), x["t"]))[:ISSUE_N]
    rise = None
    if b_all:
        rc = [x for x in out if x["n"] >= 2 and (x.get("r") is None or x["r"] >= 2)]
        rise = sorted(rc, key=lambda x: (-x["m"] * min(x.get("r") or 5, 5) * math.log(x["n"] + 1), x["t"]))[:ISSUE_N]
    return top, rise


def main():
    if not TOKEN and not DRY:
        sys.exit("INGEST_TOKEN 이 없습니다.")
    t0 = time.time()
    today = NOW.date()
    since_date = today - timedelta(days=DAYS - 1)
    since = f"{since_date}T00:00"
    load_since = min(since, f"{today - timedelta(days=LOAD_DAYS - 1)}T00:00")  # 이슈 묶기·3일·7일용 최근 7일

    jobs = [(c, s, u) for c, lst in FEEDS.items() for s, u in lst]
    with ThreadPoolExecutor(8) as ex:
        results = list(ex.map(fetch, jobs))
    status = [st for st, _ in results]
    fetched = [it for _, items in results for it in items if it["pub"] >= since]
    print(f"RSS {sum(s['ok'] for s in status)}/{len(status)}개 응답, 기간 안 항목 {len(fetched)}건")
    for s in status:
        if not s["ok"]:
            print(f"  ::warning::{s['cat']} {s['src']} 실패 — {s['error']}")

    existing = [] if DRY else load_recent(load_since)
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
    # 일별 집계: 이번에 다시 본 날짜만 새로 만들고(rekey 면 불러온 기간 전체), 그 전 날짜는 저장된 것 사용
    by_day = collections.defaultdict(list)
    for a in arts:
        by_day[a["pub"][:10]].append(a)
    redo_from = min(since_date, today - timedelta(days=LOAD_DAYS - 1)) if REKEY else since_date
    redo = [str(redo_from + timedelta(days=i)) for i in range((today - redo_from).days + 1)]
    fresh = {d: daily_body(by_day.get(d, [])) for d in redo}
    stored = {} if DRY else load_daily(str(today - timedelta(days=30 + BASE_DAYS + 7)))
    daily = {**stored, **fresh}
    if not DRY:
        rows = [{"date": d, "body": json.dumps(b, ensure_ascii=False, separators=(",", ":"))} for d, b in fresh.items()]
        for i in range(0, len(rows), 5):
            api("PUT", "/api/daily", json={"rows": rows[i:i + 5]})

    def day(i):
        return str(today - timedelta(days=i))

    trend_dates = [day(i) for i in range(TREND_DAYS - 1, -1, -1)]

    def trend_fn(cat, kind):
        return lambda k: [daily.get(d, {}).get(kind, {}).get(cat, {}).get(k, [0])[0] for d in trend_dates]

    _, order_all = stats_from_arts(arts)

    def disp(k):
        return order_all[k].most_common(1)[0][0] if order_all.get(k) else k

    out = {"generatedAt": fmt(NOW), "cats": CATS, "trendDates": trend_dates, "periods": [], "data": {}}
    for key, label, n_days in PERIODS:
        if n_days is None:
            frm = fmt(NOW - timedelta(hours=24))
            base_range = range(2, 2 + BASE_DAYS)
        else:
            frm = f"{day(n_days - 1)}T00:00"
            base_range = range(n_days, n_days + BASE_DAYS) if n_days <= 7 else range(0)
        from_arts = n_days is None or n_days <= LOAD_DAYS
        if from_arts:
            sub = [a for a in arts if a["pub"] >= frm]
            cur, _ = stats_from_arts(sub)
        else:
            sub = []
            cur = merge_days([daily[day(i)] for i in range(n_days) if day(i) in daily])
        base_days = [daily[day(i)] for i in base_range if day(i) in daily]
        base = merge_days(base_days) if len(base_days) >= 3 else None
        out["periods"].append({"key": key, "label": label, "from": frm, "baseDays": len(base_days),
                               "rising": base is not None, "issues": from_arts})
        out["data"][key] = {}
        by_cat = collections.defaultdict(list)
        for i, a in enumerate(sub):
            by_cat[a["cat"]].append(i)
        for c in CATS:
            r, b = cur[c], (base[c] if base else None)
            wb = {"N": b["N"], "cnt": b["w"][0]} if b else None
            pb = {"N": b["N"], "cnt": b["p"][0]} if b else None
            w_top, w_rise = rank(r["N"], r["w"], wb, trend_fn(c, "w"))
            p_top, p_rise = rank(r["N"], r["p"], pb, trend_fn(c, "p"), disp, dedupe=True)
            blk = {"count": r["N"], "words": {"top": w_top, "rise": w_rise}, "pairs": {"top": p_top, "rise": p_rise}}
            if from_arts:
                i_top, i_rise = issues(sub, by_cat[c], pb, r["N"])
                blk["issues"] = {"top": i_top, "rise": i_rise}
            out["data"][key][c] = blk

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
    print(f"이슈·키워드 순위 저장 ({len(text) // 1024}KB), {round(time.time() - t0)}초")


if __name__ == "__main__":
    main()
