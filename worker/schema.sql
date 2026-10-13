-- 이슈 키워드 D1 스키마 — 적용: npx -y wrangler@4 d1 execute issue-keyword-db --remote --file schema.sql

-- 기사 (제목·링크만, 본문 없음). pub = 발행시각 KST 'YYYY-MM-DDTHH:MM', kws = 키워드 공백 구분
CREATE TABLE IF NOT EXISTS articles (
  id        TEXT PRIMARY KEY,
  cat       TEXT NOT NULL,
  src       TEXT NOT NULL,
  title     TEXT NOT NULL,
  url       TEXT NOT NULL,
  pub       TEXT NOT NULL,
  kws       TEXT NOT NULL DEFAULT '',
  tkey      TEXT NOT NULL,
  collected TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_tkey ON articles (tkey);
CREATE INDEX IF NOT EXISTS idx_articles_cat_pub ON articles (cat, pub);
CREATE INDEX IF NOT EXISTS idx_articles_pub ON articles (pub);

-- 일별·분야별 단어 집계 (수집 스크립트가 만듦). body = {"total":{분야:기사 수},"w":{분야:{단어:[기사 수,"매체|매체"]}}}
CREATE TABLE IF NOT EXISTS daily (
  date    TEXT PRIMARY KEY,
  body    TEXT NOT NULL,
  updated TEXT NOT NULL
);

-- 화면용 결과 (snapshot = 기간별 키워드 순위 + 수집 현황)
CREATE TABLE IF NOT EXISTS kv (
  k       TEXT PRIMARY KEY,
  body    TEXT NOT NULL,
  updated TEXT NOT NULL
);
