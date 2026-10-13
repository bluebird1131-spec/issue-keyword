# 이슈 키워드

주요 언론사 기사를 분야(정치·경제·사회·IT·연예·스포츠·건강·문화)별로 모아, 지금 이슈가 되는 키워드를 워드클라우드·순위로 보여주는 사이트.

- 화면: https://issue-keyword.bluebird1131.workers.dev (병원망에서 열리는 *.workers.dev)
- 접속 암호: Worker Secret `SITE_PASSWORD` (확인되면 30일 쿠키, 암호를 바꾸면 모두 다시 로그인)
- 매체: 연합뉴스·조선일보·동아일보(스포츠동아)·한겨레·SBS·한국경제·YTN·뉴시스·전자신문·지디넷코리아. YTN 은 RSS 가 없어 목록 JSON 사용
- 기사 본문은 저장·게시하지 않음. 제목·매체·발행시각·원문 링크만

## 구조

```
[01 Worker(medical-trend-api) 예약] 매시 정각 → collect.yml (workflow_dispatch)
        ▼
[GitHub Actions · scripts/collect.py]  분야별 RSS 수집 → 제목 정리 → Kiwi 명사 추출
        │   새 기사·일별 집계·키워드 순위(snapshot) → Worker API (INGEST_TOKEN)
        ▼
[Cloudflare D1 issue-keyword-db]  articles / daily / kv
        ▼
[Cloudflare Worker issue-keyword]  public/index.html + /api/snapshot, /api/articles
```

## 설정 고치기 (config/)

| 파일 | 내용 |
|---|---|
| `feeds.json` | 분야 → [매체, RSS 주소]. 분야 순서가 화면 순서 |
| `stopwords.txt` | 키워드에서 뺄 단어 |
| `userdict.txt` | 한 단어로 묶을 복합어·신조어 |
| `synonyms.json` | 같은 뜻 묶기, 한자 약칭(與·野·北) 바꾸기 |

고친 뒤 커밋하면 다음 수집부터 반영. 이미 모은 기사에도 적용하려면 Actions → 기사 수집 및 키워드 집계 → Run workflow → `rekey` 에 `1`, `days` 에 `30`.

## 이슈·키워드 순위

- **이슈 묶음**: 같은 분야에서 핵심 단어 2개 이상을 공유하고, 드문 단어(사람·사건명)에 가중치를 둔 공통 비율이 0.4 이상인 기사끼리 묶음(`CLUSTER_TH`). 2곳 이상 매체가 다룬 묶음만, `매체 수 × log(기사 수+1)` 순. 대표 제목·핵심 단어 3개·기사 번호(최대 40) 저장. 최근 24시간·3일·7일만 (30일은 키워드만)
- **두 단어 묶음**: 한 제목 안에 함께 나온 키워드 짝(코스피 급락). 같은 기사들을 가리키는 짝은 하나만 남김(겹침 60% 이상·삼각 중복 제외). 워드클라우드 기본값
- **한 단어**: 2곳 이상 매체가 다룬 단어를 `기사 수 × (1 + 매체 수/4)` 로 정렬
- **급상승**: 기간 안 비중을 직전 14일 평균 비중과 비교 (×배수). 비교 자료가 3일 이상 쌓이면 표시
- 24시간·3일·7일은 최근 7일 기사로 직접 계산, 30일은 일별 집계(단어·두 단어) 합산

## 배포·운영

- Worker: `cd worker && npx -y wrangler@4 deploy` / 스키마: `npx -y wrangler@4 d1 execute issue-keyword-db --remote --file schema.sql`
- Secrets: Worker `INGEST_TOKEN` = Actions Secret `INGEST_TOKEN` (바꿀 때는 둘 다), Worker `SITE_PASSWORD`
- 암호 바꾸기: `cd worker && npx -y wrangler@4 secret put SITE_PASSWORD`
- 로컬 실행: `pip install -r requirements.txt` 후 `INGEST_TOKEN=… python scripts/collect.py` (`DRY=1` 이면 저장 없이 snapshot.local.json)
