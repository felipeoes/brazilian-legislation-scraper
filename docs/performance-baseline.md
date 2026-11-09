# Performance baseline

A reference live run of all 30 scrapers. Use it to judge whether a later change made scraping faster or slower. Regenerate the table with `uv run main.py stats`; every run stores these numbers in `runs.stats`.

## Setup

- Date: 2026-11-09, branch `refactor/sqlite-bounded-scrapers` (PR #6).
- Machine: AMD Ryzen Threadripper PRO 5975WX (32 cores, 64 threads), 125 GB RAM, Linux 7.0, Python 3.14.2, archives on a local LVM disk.
- OCR: `LLM_MODEL=anthropic:claude-haiku-5-5`, default `LLM_CONCURRENCY` and `LLM_BATCH_SIZE`. PydanticAI needs the `anthropic:` prefix.
- Fresh archives (no resume), default registry settings (workers, rps, retries).

Commands:

```bash
export LLM_MODEL=anthropic:claude-haiku-5-5
# 24 sources at once, one process each (--jobs defaults to all of them)
uv run main.py run -s CamaraDep Conama AC AL AM AP BA CE DF ES GO MA MS MG PA PB PE PI RN RO RR SP SE TO --year 2012
uv run main.py run -s RJ SC RS --year 2012
uv run main.py run -s MT --year 2018      # source starts in 2017
uv run main.py run -s ICMBio --year 2018  # source starts in 2016
uv run main.py run -s PR --year 2025      # source starts in 2025
uv run main.py stats
```

MA, GO, RO, PB and PI were rerun alone after fixes made during the run. Their rows are from those reruns. The 24-source batch took 96 minutes of wall time (00:53 to 02:29), bound by Espírito Santo.

## Reading the columns

- **Seconds**: wall time of the run. **Listing s**: time until the last reference was queued. The queue is bounded, so this includes waiting for workers and is close to Seconds for most sources.
- **Docs/min**: saved documents per minute of wall time.
- **Fetch p50/p95/max**: seconds per document, from fetch start to saved row. It includes waits on the shared rate limiter, so a source with many workers and a low `rps` shows long fetch times while the network is idle.
- **HTTP req** counts every attempt, **Retries** the attempts after the first, **MB** the downloaded bytes. **LLM req** counts OCR requests.

## Results

| Scraper | Years | Listed | Docs | Errors | Seconds | Listing s | Docs/min | Fetch p50 s | Fetch p95 s | Fetch max s | HTTP req | Retries | MB | Browser navs | LLM req |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| LEGISLACAO_FEDERAL | 2012-2012 | 1698 | 1692 | 6 | 290 | 281 | 350.6 | 2.35 | 4.84 | 9.62 | 3477 | 2 | 152.7 | 0 | 0 |
| CONAMA | 2012-2012 | 24 | 24 | 0 | 6 | 0 | 229.1 | 2.5 | 4.29 | 4.58 | 25 | 0 | 3.4 | 0 | 0 |
| ICMBIO | 2018-2018 | 24 | 24 | 0 | 7 | 2 | 219.0 | 2.23 | 3.08 | 4.16 | 28 | 0 | 3.1 | 0 | 0 |
| ACRE | 2012-2012 | 166 | 166 | 0 | 28 | 20 | 362.2 | 2.4 | 2.42 | 2.59 | 167 | 0 | 20.1 | 0 | 0 |
| ALAGOAS | 2012-2012 | 6326 | 6318 | 8 | 1771 | 1738 | 214.1 | 3.99 | 7.98 | 45.61 | 6999 | 40 | 527.9 | 0 | 0 |
| AMAZONAS | 2012-2012 | 191 | 191 | 0 | 70 | 69 | 163.0 | 0.94 | 1.35 | 1.44 | 210 | 0 | 9.8 | 0 | 0 |
| AMAPA | 2012-2012 | 131 | 131 | 0 | 21 | 20 | 367.1 | 2.41 | 2.65 | 3.15 | 141 | 0 | 1.9 | 0 | 0 |
| BAHIA | 2012-2012 | 1138 | 1132 | 6 | 4590 | 4538 | 14.8 | 44.99 | 213.92 | 670.73 | 4372 | 1 | 44.9 | 0 | 0 |
| CEARA | 2012-2012 | 209 | 205 | 4 | 72 | 28 | 169.7 | 2.42 | 4.9 | 8.15 | 475 | 0 | 48.8 | 0 | 0 |
| DISTRITO_FEDERAL | 2012-2012 | 2479 | 2446 | 33 | 175 | 153 | 839.4 | 6.27 | 12.32 | 24.38 | 2907 | 0 | 184.5 | 0 | 0 |
| ESPIRITO_SANTO | 2012-2012 | 2981 | 2981 | 0 | 5774 | 5763 | 31.0 | 10.38 | 133.23 | 406.5 | 4247 | 249 | 151.4 | 0 | 0 |
| GOIAS | 2012-2012 | 1132 | 1131 | 1 | 72 | 68 | 944.7 | 0.96 | 1.18 | 6.13 | 1163 | 0 | 36.3 | 0 | 0 |
| MARANHAO | 2012-2012 | 271 | 270 | 1 | 47 | 47 | 341.9 | 2.21 | 2.82 | 28.81 | 311 | 0 | 10.8 | 0 | 0 |
| MATO_GROSSO_DO_SUL | 2012-2012 | 409 | 409 | 0 | 70 | 70 | 351.0 | 2.41 | 4.84 | 5.29 | 466 | 0 | 6.5 | 0 | 0 |
| MATO_GROSSO | 2018-2018 | 1005 | 1005 | 0 | 530 | 506 | 113.9 | 8.01 | 11.76 | 13.06 | 2112 | 0 | 88.7 | 0 | 0 |
| MINAS_GERAIS | 2012-2012 | 1786 | 1786 | 0 | 577 | 556 | 185.8 | 4.81 | 7.28 | 13.71 | 3752 | 0 | 402.6 | 0 | 0 |
| PARA | 2012-2012 | 410 | 410 | 0 | 134 | 105 | 184.2 | 3.98 | 6.05 | 37.83 | 455 | 0 | 88.6 | 0 | 0 |
| PARAIBA | 2012-2012 | 327 | 327 | 0 | 380 | 374 | 51.7 | 2.1 | 3.74 | 108.22 | 333 | 0 | 257.3 | 0 | 29 |
| PARANA (partial) | 2025-2025 | 1158 | 1154 | 1 | 6355 | — | 10.9 | 10.1 | 15.16 | 146.45 | 1230 | 5 | 18.6 | 0 | 0 |
| PERNAMBUCO | 2012-2012 | 1731 | 1731 | 0 | 569 | 551 | 182.5 | 4.8 | 7.16 | 44.92 | 3496 | 0 | 443.9 | 0 | 0 |
| PIAUI | 2012-2012 | 119 | 119 | 0 | 64 | 21 | 111.1 | 2.49 | 16.32 | 44.7 | 122 | 0 | 37.2 | 0 | 37 |
| RIO_DE_JANEIRO | 2012-2012 | 510 | 510 | 0 | 99 | 99 | 307.7 | 2.4 | 2.57 | 3.07 | 564 | 0 | 76.4 | 0 | 0 |
| RIO_GRANDE_DO_NORTE | 2012-2012 | 98 | 97 | 1 | 82 | 11 | 70.6 | 2.37 | 6.21 | 24.19 | 115 | 5 | 9.8 | 0 | 0 |
| RIO_GRANDE_DO_SUL | 2012-2012 | 1651 | 1648 | 3 | 1006 | 974 | 98.3 | 9.64 | 11.37 | 26.15 | 4983 | 0 | 157.2 | 0 | 0 |
| RONDONIA | 2012-2012 | 1335 | 1331 | 4 | 1508 | 1418 | 52.9 | 12.23 | 49.39 | 1040.89 | 1339 | 0 | 1148.7 | 0 | 1204 |
| RORAIMA | 2012-2012 | 128 | 128 | 0 | 23 | 14 | 328.8 | 2.25 | 4.04 | 7.69 | 131 | 0 | 135.2 | 0 | 0 |
| SANTA_CATARINA | 2012-2012 | 601 | 601 | 0 | 116 | 116 | 311.2 | 1.58 | 2.16 | 5.01 | 726 | 0 | 37.5 | 0 | 0 |
| SAO_PAULO | 2012-2012 | 1656 | 1654 | 2 | 348 | 342 | 285.1 | 2.52 | 5.3 | 20.6 | 2305 | 0 | 167.2 | 0 | 0 |
| SERGIPE | 2012-2012 | 914 | 912 | 2 | 331 | 270 | 165.2 | 4.71 | 5.42 | 57.75 | 1858 | 10 | 91.8 | 0 | 0 |
| TOCANTINS | 2012-2012 | 145 | 145 | 0 | 131 | 23 | 66.2 | 2.15 | 9.85 | 125.59 | 155 | 0 | 25.5 | 0 | 0 |

Paraná was stopped with Ctrl-C after 106 minutes; a full 2025 run takes several hours at its 0.2 rps limit (about 11 documents per minute). Its listing in this run was also incomplete: see the remaining errors below.

No scraper navigates a browser anymore: every source is plain HTTP.

## Where the time goes

- **Rate limits.** Most sources finish at roughly `docs / rps` seconds, so their configured `rps` is the ceiling. Bahia (rps 1, about 4 requests per document), Paraíba (rps 1) and Paraná (rps 0.2, above which the site blocks clients for about 20 minutes) are slow on purpose. Raise `rps` only after checking that the site tolerates it.
- **Server latency.** Espírito Santo's server answers slowly and often fails transiently: 249 retries, p95 of 133 s, with the scraper process near 0% CPU. More workers would not help.
- **OCR.** Rondônia publishes scanned PDFs. 1,204 OCR requests dominate its run, and the longest document took 17 minutes.
- **CPU.** PDF text extraction runs in a process pool, so no source is CPU-bound. Before this change a slow regex in one scraper froze every scraper in the shared event loop.

## OCR usage and cost

| Scraper | LLM requests | Input tokens | Output tokens | Cost (USD) |
|---|---|---|---|---|
| RONDONIA | 1204 | 10.37M | 3.47M | 2.77 |
| PARAIBA | 29 | | | 0.10 |
| PIAUI | 37 | | | 0.09 |
| Total | 1270 | | | 2.96 |

The unreadable-text-layer gate (`readable_text`) sent 13 of about 11,700 text-layer PDFs (111 pages) to OCR: 12 in Paraíba, 1 in Piauí.

## Compared with the first run

The first live run (same machine, same years, before the fixes in PR #6) ran every scraper in one process. Its times are inflated: one scraper's regex froze the shared event loop, and several sources ran only partially or resumed. Compare documents and errors before times.

| Scraper | Docs before → after | Errors before → after | Seconds before → after |
|---|---|---|---|
| AMAPA | 0 → 131 | 1 → 0 | |
| PARA | 0 → 410 | 0 → 0 | |
| SAO_PAULO | 0 → 1654 | 1 → 2 | |
| SERGIPE | 0 → 912 | 5 → 2 | |
| BAHIA | 0 → 1132 | 1 → 6 | |
| ESPIRITO_SANTO | 1674 → 2981 | 0 → 0 | |
| RIO_DE_JANEIRO | 342 → 510 | 11 → 0 | |
| RIO_GRANDE_DO_NORTE | 10 → 97 | 0 → 1 | 105 → 82 |
| ICMBIO | 20 → 24 | 4 → 0 | 186 → 7 |
| PIAUI | 112 → 119 | 22 → 0 | 401 → 64 |
| PARANA (2025, partial) | 28 → 1154 | 26 → 1 | |
| ALAGOAS | 6311 → 6318 | 15 → 8 | 1757 (partial) → 1771 |
| DISTRITO_FEDERAL | 2446 → 2446 | 48 → 33 | |
| AMAZONAS | 191 → 191 | 4 → 0 | |
| CONAMA | 24 → 24 | 0 → 0 | 135 → 6 |
| MARANHAO | 270 → 270 | 2 → 1 | 628 → 47 |
| MATO_GROSSO_DO_SUL | 409 → 409 | 0 → 0 | 1032 → 70 |
| RORAIMA | 128 → 128 | 0 → 0 | 711 → 23 |
| TOCANTINS | 145 → 145 | 0 → 0 | 723 → 131 |
| MATO_GROSSO | 1004 → 1005 | 1 → 0 | 962 → 530 |
| PARAIBA | 327 → 327 | 0 → 0 | 541 → 380 |
| PERNAMBUCO | 1730 → 1731 | 1 → 0 | 883 (partial) → 569 |
| MINAS_GERAIS | 1785 → 1786 | 1 → 0 | 565 → 577 |

Other quality changes: Piauí's documents under 300 characters fell from 23 to 0, and Paraíba's from 4 to 0. Goiás lost its stray C1 control characters (28 → 0). Rio de Janeiro no longer embeds a site icon in every document (342 → 0). São Paulo (458), Bahia (205), Pará (276) and Sergipe (41) now keep their inline images. No archive references a missing raw file.

## Remaining errors

Errors left in this run are source-side unless marked as fixed after the run:

- Câmara (6), Bahia (6), Ceará (4), Maranhão (1), Rondônia (4): listed documents answer HTTP 404.
- Alagoas (8), Sergipe (2): the document API answers HTTP 500.
- Distrito Federal (33): 31 ADI rows have no text on SINJ (the file page is an empty shell), plus 2 similar rows.
- São Paulo (2): ALESP publishes no text for the norm.
- Rio Grande do Sul (3): one norm has no text, one is a Word 97 `.doc` (now a clear "not supported" error), and one PDF link 404'd because the page's undeclared charset was guessed as windows-1250. That link is fixed after the run.
- Goiás (1): the API prepended an empty HTML shell to the document. Fixed after the run.
- Rio Grande do Norte (1): the norm links to the bare storage folder. It now fails without the 403 retries that cost about 75 s.
- Paraná (1): an empty result for "Constituição Estadual" was reported as a pagination error. Fixed after the run.
- Paraná, silently: the site sorts results by publication date without a tiebreak, so consecutive pages overlap and skip acts that share a date. The run listed 649 of the 682 laws shown for 2025 (159 duplicate rows were skipped). The listing now queries act-number ranges that each fit one page, and fails visibly if some acts have no number to query by. (The site's count of 683 itself includes one phantom: Lei 22.530 is counted twice but shown once.)

Verified live after the fixes (2012, scratch archives): Goiás 1,132 docs, 0 errors, 72 s. Rio Grande do Norte 97 docs, 1 error, 34 s, down from 82 s. Rio Grande do Sul 1,649 docs, 2 errors.

A final live smoke on the committed code listed the same year for all 30 scrapers and fetched the first 8 documents each, without OCR. Of the 240 documents, 233 had byte-identical markdown to this run and none differed. The 7 errors are expected: 6 scanned PDFs need OCR, and São Paulo publishes no text for 1 norm.
