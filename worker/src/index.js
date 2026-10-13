/**
 * 이슈 키워드 API (Cloudflare Worker) — 화면(public/)은 Worker 정적 자산으로 같이 제공
 * 병원망은 구글을 막지만 *.workers.dev 는 열려 있어 화면·데이터 모두 여기서 제공함.
 *
 * 접속 암호 (SITE_PASSWORD) — 확인되면 30일 쿠키. 암호를 바꾸면 기존 쿠키는 모두 무효
 *   POST /api/login {password} / POST /api/logout
 *   암호 없이 열리는 것: 로그인 화면, /assets/* (글꼴·아이콘), 수집 스크립트 전용 API
 * 암호 확인 후
 *   GET /api/snapshot                     기간별·분야별 키워드 순위 + 수집 현황 (수집 스크립트가 매시간 만듦)
 *   GET /api/articles?cat&kw&src&q&from&to&limit&offset   기사 목록 (제목·매체·시각·링크)
 * 수집 스크립트 전용 (Authorization: Bearer INGEST_TOKEN)
 *   GET  /api/recent?since&after&limit     기간 안 기사 (중복 확인·집계용)
 *   POST /api/ingest {articles}            새 기사 저장 (같은 주소·같은 제목은 무시)
 *   POST /api/rekey  {articles:[{id,kws}]} 키워드 다시 계산한 결과 덮어쓰기 (500건씩)
 *   GET  /api/daily?since                  일별 집계 ("날짜\t본문" 줄 단위)
 *   PUT  /api/daily {rows:[{date,body}]}   일별 집계 저장
 *   PUT  /api/kv/snapshot                  화면용 결과 저장
 *
 * Secrets: INGEST_TOKEN (GitHub Actions Secret 과 같은 값), SITE_PASSWORD (팀원 접속 암호)
 * 무료 플랜 요청당 CPU 10ms — 큰 JSON 은 파싱하지 않고 문자열 그대로 저장·전달
 */
const KV_KEYS = ['snapshot'];
const BOT_PATHS = /^\/api\/(recent|ingest|rekey|daily|kv\/[a-z]+)$/;
const COOKIE = 'ik_auth', COOKIE_DAYS = 30;

export default {
  async fetch(req, env) {
    const url = new URL(req.url), p = url.pathname;
    try {
      if (p.startsWith('/assets/')) return env.ASSETS.fetch(req);
      if (BOT_PATHS.test(p)) return await route(req, env, url);
      if (p === '/api/login' && req.method === 'POST') return await login(req, env);
      if (p === '/api/logout' && req.method === 'POST') return json({ ok: true }, 200, { 'set-cookie': `${COOKIE}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax` });
      if (!(await signedIn(req, env))) {
        if (p.startsWith('/api/')) return json({ error: '접속 암호가 필요합니다.' }, 401);
        return new Response(LOGIN_HTML, { status: 401, headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' } });
      }
      if (!p.startsWith('/api/')) return env.ASSETS.fetch(req);
      return await route(req, env, url);
    } catch (e) {
      return json({ error: String(e.message || e) }, 500);
    }
  },
};

/** 쿠키 값 = HMAC(암호, 고정 문구). 암호가 바뀌면 값도 바뀌어 다시 로그인 */
async function authValue(env) {
  if (!env.SITE_PASSWORD) throw new Error('SITE_PASSWORD 가 설정되지 않았습니다.');
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(env.SITE_PASSWORD), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode('issue-keyword-site-v1'));
  return [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join('');
}

async function signedIn(req, env) {
  const m = (req.headers.get('cookie') || '').match(/(?:^|;\s*)ik_auth=([0-9a-f]{64})/);  // ik_auth = COOKIE
  return !!m && safeEqual(m[1], await authValue(env));
}

async function login(req, env) {
  const { password = '' } = await req.json().catch(() => ({}));
  if (!safeEqual(String(password), env.SITE_PASSWORD || '')) {
    await new Promise(r => setTimeout(r, 600));  // 연속 대입 늦추기
    return json({ error: '암호가 맞지 않습니다.' }, 401);
  }
  const v = await authValue(env);
  return json({ ok: true }, 200, { 'set-cookie': `${COOKIE}=${v}; Path=/; Max-Age=${COOKIE_DAYS * 86400}; HttpOnly; Secure; SameSite=Lax` });
}

async function route(req, env, url) {
  const p = url.pathname, q = url.searchParams, m = req.method;

  if (m === 'GET' && p === '/api/snapshot') {
    const row = await env.DB.prepare('SELECT body, updated FROM kv WHERE k = ?').bind('snapshot').first();
    if (!row) return json({ error: '아직 수집 결과가 없습니다.' }, 404);
    return new Response(row.body, { headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'private, max-age=60' } });
  }

  if (m === 'GET' && p === '/api/articles') return articles(env, q);

  // ---- 수집 스크립트 전용 ----
  if (!authorized(req, env)) return json({ error: '권한이 없습니다.' }, 401);
  const now = kstNow();

  if (m === 'GET' && p === '/api/recent') {
    const since = q.get('since') || '', limit = clamp(+q.get('limit') || 1000, 1, 1000);
    const [ap, aid] = (q.get('after') || '|').split('|');
    const rs = await env.DB.prepare(
      `SELECT id, cat, src, title, url, pub, kws FROM articles
       WHERE pub >= ?1 AND (pub > ?2 OR (pub = ?2 AND id > ?3)) ORDER BY pub, id LIMIT ?4`,
    ).bind(since, ap || '', aid || '', limit).raw();
    const last = rs[rs.length - 1];
    return json({ cols: ['id', 'cat', 'src', 'title', 'url', 'pub', 'kws'], rows: rs, next: rs.length === limit ? `${last[5]}|${last[0]}` : '' });
  }

  // D1 무료 플랜은 요청당 쿼리 수가 적어서, 여러 건을 JSON 하나로 넘겨 한 문장으로 처리
  if (m === 'POST' && p === '/api/ingest') {
    const { articles: list = [] } = await req.json();
    if (list.length > 200) return json({ error: '한 번에 200건까지' }, 400);
    if (!list.length) return json({ inserted: 0 });
    const r = await env.DB.prepare(
      `INSERT OR IGNORE INTO articles (id, cat, src, title, url, pub, kws, tkey, collected)
       SELECT value ->> 'id', value ->> 'cat', value ->> 'src', value ->> 'title', value ->> 'url', value ->> 'pub',
              coalesce(value ->> 'kws', ''), value ->> 'tkey', ?2
       FROM json_each(?1)`,
    ).bind(JSON.stringify(list.map(({ id, cat, src, title, url, pub, kws, tkey }) => ({ id, cat, src, title, url, pub, kws, tkey }))), now).run();
    return json({ inserted: r.meta?.changes || 0 });
  }

  if (m === 'POST' && p === '/api/rekey') {
    const { articles: list = [] } = await req.json();
    if (list.length) {
      await env.DB.prepare(
        `UPDATE articles SET kws = (SELECT value ->> 'kws' FROM json_each(?1) WHERE value ->> 'id' = articles.id)
         WHERE id IN (SELECT value ->> 'id' FROM json_each(?1))`,
      ).bind(JSON.stringify(list.slice(0, 500).map(({ id, kws }) => ({ id, kws: kws || '' })))).run();
    }
    return json({ ok: true });
  }

  if (m === 'GET' && p === '/api/daily') {
    const { results } = await env.DB.prepare('SELECT date, body FROM daily WHERE date >= ? ORDER BY date').bind(q.get('since') || '').all();
    return new Response(results.map(r => `${r.date}\t${r.body}`).join('\n'), { headers: { 'content-type': 'text/plain; charset=utf-8' } });
  }

  if (m === 'PUT' && p === '/api/daily') {
    const { rows = [] } = await req.json();
    const st = env.DB.prepare('INSERT INTO daily (date, body, updated) VALUES (?, ?, ?) ON CONFLICT(date) DO UPDATE SET body = excluded.body, updated = excluded.updated');
    if (rows.length) await env.DB.batch(rows.map(r => st.bind(r.date, r.body, now)));
    return json({ ok: true });
  }

  const kv = p.match(/^\/api\/kv\/([a-z]+)$/);
  if (m === 'PUT' && kv && KV_KEYS.includes(kv[1])) {
    const body = await req.text();
    await env.DB.prepare('INSERT INTO kv (k, body, updated) VALUES (?, ?, ?) ON CONFLICT(k) DO UPDATE SET body = excluded.body, updated = excluded.updated')
      .bind(kv[1], body, now).run();
    return json({ ok: true, bytes: body.length });
  }

  return json({ error: '없는 주소입니다.' }, 404);
}

/** 기사 목록: 분야·키워드·매체·검색어·기간. 기간이 없으면 최근 24시간 (D1 무료 읽기 한도 보호) */
async function articles(env, q) {
  const where = [], args = [];
  const add = (sql, v) => { where.push(sql); args.push(v); };
  const from = q.get('from') || kstNow(-24 * 60), to = q.get('to');
  add('pub >= ?', from);
  if (to) add('pub < ?', to);
  if (q.get('cat')) add('cat = ?', q.get('cat'));
  if (q.get('src')) add('src = ?', q.get('src'));
  if (q.get('kw')) add("instr(' ' || kws || ' ', ?) > 0", ` ${q.get('kw')} `);
  if (q.get('q')) add('instr(title, ?) > 0', q.get('q').trim());
  const limit = clamp(+q.get('limit') || 50, 1, 100), offset = clamp(+q.get('offset') || 0, 0, 5000);
  const rs = await env.DB.prepare(
    `SELECT cat, src, title, url, pub FROM articles WHERE ${where.join(' AND ')} ORDER BY pub DESC, id LIMIT ? OFFSET ?`,
  ).bind(...args, limit + 1, offset).raw();
  return json({ rows: rs.slice(0, limit), more: rs.length > limit }, 200, { 'cache-control': 'private, max-age=60' });
}

function authorized(req, env) {
  const got = (req.headers.get('authorization') || '').replace(/^Bearer\s+/i, '');
  return !!env.INGEST_TOKEN && safeEqual(got, env.INGEST_TOKEN);
}

function safeEqual(a, b) {
  if (!b || a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < b.length; i++) d |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return d === 0;
}

function kstNow(offsetMin = 0) {
  return new Date(Date.now() + (9 * 60 + offsetMin) * 60000).toISOString().slice(0, 16);
}

const clamp = (v, a, b) => Math.max(a, Math.min(b, Math.floor(v) || a));

function json(obj, status = 200, headers = {}) {
  return new Response(JSON.stringify(obj), { status, headers: { 'content-type': 'application/json; charset=utf-8', ...headers } });
}

const LOGIN_HTML = `<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>이슈 키워드</title>
<style>
@font-face { font-family: "Pretendard Variable"; font-weight: 45 920; font-display: swap; src: url("/assets/PretendardVariable.woff2") format("woff2-variations"); }
:root { color-scheme: light; }
* { box-sizing: border-box; }
body { margin: 0; min-height: 100vh; display: grid; place-items: center; padding: 16px; background: #f7f4ec; color: #292524;
  font-family: "Pretendard Variable", -apple-system, "Malgun Gothic", sans-serif; -webkit-font-smoothing: antialiased; }
.card { width: 100%; max-width: 360px; background: #fffdf8; border: 1px solid #e8e2d4; border-radius: 16px; padding: 28px 24px 24px; }
.mark { width: 40px; height: 40px; border-radius: 10px; display: grid; place-items: center; background: #1c1917; color: #fafaf9; margin-bottom: 14px; }
h1 { margin: 0; font-size: 21px; font-weight: 800; letter-spacing: -0.02em; color: #1c1917; }
p { margin: 6px 0 18px; font-size: 14px; color: #78716c; }
input, button { font: inherit; width: 100%; height: 42px; border-radius: 10px; }
input { border: 1px solid #e8e2d4; background: #fffdf8; padding: 0 12px; font-size: 15px; }
input:focus { outline: 2px solid #a1a1aa; outline-offset: 1px; }
button { margin-top: 10px; border: 0; background: #1c1917; color: #fafaf9; font-size: 15px; font-weight: 700; cursor: pointer; }
button:hover { opacity: .88; }
.err { min-height: 20px; margin-top: 10px; font-size: 13.5px; color: #b91c1c; }
</style></head>
<body><form class="card" id="f">
<div class="mark"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="11" x="3" y="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg></div>
<h1>이슈 키워드</h1><p>접속 암호를 입력하세요</p>
<input type="password" name="password" placeholder="접속 암호" autocomplete="current-password" autofocus required>
<button type="submit">들어가기</button><div class="err" id="err"></div>
</form>
<script>
document.getElementById('f').onsubmit = async e => {
  e.preventDefault();
  const r = await fetch('/api/login', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ password: e.target.password.value }) });
  if (r.ok) location.reload(); else document.getElementById('err').textContent = (await r.json().catch(() => ({}))).error || '로그인 실패';
};
</script></body></html>`;
