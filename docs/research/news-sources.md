<!-- Research report produced on 2026-10-08 to design the news collectors. Evidence labels are explained in the text. -->

# Brazilian financial news sources for an automated archive: what I could verify (8 Oct 2026)

## How to read this

- **Verified** means I fetched the URL with curl during this session and parsed the response. Anything else is marked "not verified" or "from memory".
- **All tests ran from a Brazilian residential IP.** I could not test datacenter-IP blocking; your VPS may see different behaviour, especially on Cloudflare/Akamai-fronted sites (Investing, E-Investidor, InvestNews, IBGE).
- **User-Agent matters.** The UA `feedfetcher-personal/0.1 (+personal archive; python)` was rejected by BCB (HTTP 200 with a 5.9 KB error page), the IBGE API ("Request Rejected"), Agência Brasil (500) and the old E-Investidor feed (403). `norn-news/0.1 (personal archive)`, `curl/8.5.0` and `python-requests/2.32.3` all worked on BCB and IBGE. Validate content type and body, not just the status code.
- Paywall status is a heuristic: JSON-LD `isAccessibleForFree` plus Piano/tinypass markers in one article per outlet. I did not log in or compare against subscriber views.

## 1. Outlet feeds

All feed URLs below returned HTTP 200 with parseable RSS/Atom unless noted. "Span" is newest to oldest item at fetch time, which indicates how often you must poll.

| Outlet | Verified feed URL(s) | Body in feed | Items / span | Article page (one sample) |
|---|---|---|---|---|
| Valor Econômico | `https://valor.globo.com/rss/valor/` (advertised in page head; `https://pox.globo.com/rss/valor/` is identical). Sections: `https://pox.globo.com/rss/valor/financas/`, `.../valor/brasil/`, `.../valor/empresas/` | Long body in `<description>` (median about 2.6k chars plain, max 8.7k). Not checked whether complete. No author; one category | 100 items. Main about 10 h; finanças about 39 h; empresas about 38 h; brasil about 6.5 days | 200 to curl; Piano/tinypass paywall scripts present |
| Valor Investe | `https://pox.globo.com/rss/valorinveste/` | Long body in description (avg 3.7k raw) | 100 / about 6.6 days | Same Globo paywall markers |
| Pipeline (Valor) | `https://pipelinevalor.globo.com/rss/pipelinevalor/` (`pox.globo.com/rss/pipeline/` gives 400) | Long body in description (avg 3.9k raw) | 100 / about 30 days | Same Globo paywall markers |
| O Globo Economia | `https://oglobo.globo.com/rss/oglobo/economia/` (same at `pox.globo.com/rss/oglobo/economia/`) | Long body in description (avg 3.9k raw) | 100 / about 2.2 days | Piano/tinypass markers |
| G1 Economia | `https://g1.globo.com/rss/g1/economia/` (`g1.globo.com/dynamo/economia/rss2.xml` redirects to it) | Full-looking body in description (median 4.4k chars plain), plus `atom:subtitle` | 100 / about 5.7 days; includes lottery and tech items | 200; free |
| InfoMoney | `https://www.infomoney.com.br/feed/`. Advertised `/mercados/feed/` and `/economia/feed/` return valid RSS with 0 items | Full text in `content:encoded` (avg 5.8k raw); author, categories | 10 per page / about 2.3 h. `?paged=N` works; page 500 reached 29 Aug 2026 | 200; no paywall markers |
| Estadão Economia | `https://www.estadao.com.br/arc/outboundfeeds/feeds/rss/sections/economia/` (advertised in page head) | Full text in `content:encoded` (avg 5.4k raw); author; description empty | 20 / about 8 h. `?size=` and `?from=` are ignored | 200; JSON-LD has both `True` and `False`, plus "assine para" text: metered or partial paywall |
| Estadão E-Investidor | `https://www.estadao.com.br/arc/outboundfeeds/feeds/rss/sections/einvestidor/`. The old `https://einvestidor.estadao.com.br/feed/` is stale (newest item 27 May 2026); do not use | Full text in `content:encoded` (avg 5.7k raw) | 20 items; newest 8 Oct 2026, but one item dated 31 Dec 2025, so ordering is not strictly chronological | Same as Estadão |
| Folha Mercado | `https://feeds.folha.uol.com.br/mercado/rss091.xml`; all sections at `.../emcimadahora/rss091.xml`. Index of feeds at `https://www1.folha.uol.com.br/feed/` | Summary only (about 800 chars raw, HTML-escaped); author present. Links are wrapped as `redir.folha.com.br/redir/online/mercado/rss091/*<real url>`. Date format is non-standard (`07 Oct 2026 23:00:00 -0300`) | 100 / about 32 h | `isAccessibleForFree: false`; "exclusivo para assinantes" |
| Exame | `https://exame.com/feed/` (teaser only, about 375 chars plus a "Leia mais" link) and `https://exame.com/invest/feed/` (full text in a `<content>` element, avg 9.9k raw). `/economia/feed/`, `/brasil/feed/`, `/mercados/feed/` return 404 | See left. No author. Dates have no timezone (`2026-10-07T22:04:13`) | 25 each; `/feed/` about 4 h, `/invest/feed/` about 36 h. `?paged=` ignored | `isAccessibleForFree: true` |
| Bloomberg Línea Brasil | `https://www.bloomberglinea.com.br/arc/outboundfeeds/rss/?outputType=xml`; section: `.../arc/outboundfeeds/rss/category/mercados/?outputType=xml` | Full text in `content:encoded` (avg 8.4k raw); author | 100 / about 36 days (main); mercados back to 28 May 2026. `from=` and `size=` ignored | `isAccessibleForFree: true`; Piano script present |
| Brazil Journal | `https://braziljournal.com/feed/` | Full text in `content:encoded` (avg 5.1k raw); author, categories | 10 / about 31 h; `?paged=100` reached Mar 2026 | 200; no paywall markers |
| NeoFeed | `https://neofeed.com.br/feed/` | Summary only (description about 1.1k raw); author, categories | 10 / about 12 h; `?paged=100` reached May 2026 | 200; no paywall markers |
| Money Times | `https://www.moneytimes.com.br/feed/` | Full text in `content:encoded` (avg 3.5k raw) | 10 / about 2 h; `?paged=200` reached 14 Sep 2026 | 200; no markers |
| Seu Dinheiro | `https://www.seudinheiro.com/feed/` | Full text in `content:encoded` (avg 8.7k raw) | 10 / about 5.5 h; `?paged=200` reached 6 Jul 2026 | 200; no markers |
| Investing.com Brasil | `https://br.investing.com/rss/news.rss`, `.../rss/news_14.rss` (channel title "Notícias sobre Economia"), `.../rss/news_25.rss`, `.../rss/market_overview.rss`. More are listed at `https://br.investing.com/webmaster-tools/rss` | Title, link, date and author only; no description. Dates have no timezone | 10 / about 1 h (news.rss) | 403 Cloudflare "Just a moment" to curl even with a browser UA: needs JavaScript |
| Reuters (Brazil/LatAm) | **No feed verified.** The `reutersagency.com/feed/...` and `reuters.com/arc/outboundfeeds/v3/all/` patterns return 404 | n/a | n/a | Section page and ToS page return 401 to curl. Skip |
| CNN Brasil | `https://www.cnnbrasil.com.br/feed/` (redirects to `admin.cnnbrasil.com.br/feed/`); all sections mixed. No economy-only feed found (`/economia/feed/` is 404) | Full text in `content:encoded` (avg 8k raw); author, categories (filter on these) | 60 / about 4.5 h; `?paged=` not supported | `isAccessibleForFree: true` |
| UOL Economia | `https://rss.uol.com.br/feed/economia.xml` | Summary (about 380 chars). Charset ISO-8859-1; dates in Portuguese (`Qua, 07 Out 2026 18:31:37 -0300`), so a custom date parser is needed | 15 / about 4 h | `isAccessibleForFree: true` on sample; Piano present |
| Poder360 | `https://www.poder360.com.br/feed/` (all sections). Section feeds are dead: `/poder-economia/feed/` has 1 item from 2024; `/category/economia/feed/` newest is Mar 2025 | Full text in `content:encoded` (avg 4k raw); many categories | 10 / about 2.7 h; `?paged=` ignored (same page returned) | `isAccessibleForFree: true` |
| Agência Brasil Economia | `https://agenciabrasil.ebc.com.br/rss/economia/feed.xml`; all: `.../rss/ultimasnoticias/feed.xml` | Full text in description (avg 6.2k raw); author, categories | 10 / about 14 h; `?page=2` ignored | Free, public agency |
| Suno Notícias | `https://www.suno.com.br/noticias/feed/` | Full text in `content:encoded` (avg 4.5k raw) | 10 / about 8.5 h; `?paged=200` reached May 2026 | 200; no markers |
| InvestNews | `https://investnews.com.br/feed/`. The advertised `/economia/feed/` is stale (Sep 2026) and has no body | Full text in `content:encoded` (avg 9.2k raw) | 30 / about 33 h; `?paged=50` reached Jun 2026 | `isAccessibleForFree: True` |
| The Brazilian Report | **No feed verified.** `brazilian.report` is now a 6-page Framer landing site. Content lives at `https://newsletters.brazilian.report/`, where `/feed` and `/rss` return HTML | n/a | Sitemap only (see section 3) | Subscription product (there is an `/upgrade` link); per-article paywall not checked |

Polling: InfoMoney, Money Times, Poder360, Investing, CNN and UOL roll over in 1 to 5 hours, so poll every 15 to 30 minutes. Where `?paged=` works you can catch up after downtime instead.

## 2. Official and primary sources

| Source | Verified endpoint | Format and content | Bulk / backfill |
|---|---|---|---|
| BCB feeds | `https://www.bcb.gov.br/api/feed/sitebcb/sitefeeds/{name}` with name in `noticias`, `notasImprensa`, `comunicadoscopom`, `atascopom`, `focus`, `ri` (RPM), `ref`, `cambio`. The names `normativos`, `discursos`, `rpm`, `relatorioinflacao` return 400 | Atom, 10 entries each, full HTML in `<content>` (atas avg 22k chars, comunicados about 7k). Links look like `https://www.bcb.gov.br/detalhenoticia/21282/nota` | Feeds hold only the last 10; `?ano=` is ignored |
| BCB Copom JSON | `https://www.bcb.gov.br/api/servico/sitebcb/copom/comunicados?quantidade=1000`, `.../copom/atas?quantidade=1000`; full text at `.../copom/comunicados_detalhes?nro_reuniao=281` and `.../copom/atas_detalhes?nro_reuniao=281` | JSON. Details carry `textoComunicado` / `textoAta` (HTML) and `urlPdfAta` | Complete: 236 comunicados back to meeting 46 (19 Apr 2000); 261 atas back to meeting 21 (Jan 1998) |
| BCB notas à imprensa / notícias archive | Not verified; I found no listing API beyond the 10-item feeds | n/a | Could not verify |
| CVM filings (fatos relevantes, comunicados ao mercado) | `https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{YYYY}.zip` (dataset page `https://dados.cvm.gov.br/dataset/cia_aberta-doc-ipe`) | One CSV per year, `;`-separated, latin-1. Columns: `CNPJ_Companhia; Nome_Companhia; Codigo_CVM; Data_Referencia; Categoria; Tipo; Especie; Assunto; Data_Entrega; Tipo_Apresentacao; Protocolo_Entrega; Versao; Link_Download`. The 2026 file has 36,032 rows, of which 1,953 "Fato Relevante" and 4,422 "Comunicado ao Mercado". Latest `Data_Entrega` was 2026-10-03 (five days before the test). `Link_Download` points to `https://www.rad.cvm.gov.br/ENET/frmDownloadDocumento.aspx?...`, which I did not fetch | Yearly files 2003 to 2026. Licence shown on the dataset page: ODbL. robots.txt: `Disallow: /api/`, `Crawl-delay: 10` |
| CVM notícias | `https://www.gov.br/cvm/pt-br/@@search_rss?portal_type:list=News%20Item&sort_on=Date&sort_order=reverse` | RSS 1.0 (RDF), 15 newest site items, title/link/date only. The type filter is not applied (PDFs and atas are mixed in), so filter on the `/assuntos/noticias/` path. `.../assuntos/noticias/RSS` is stale (2024 items) | `b_start` is ignored on the feed. The HTML listing `https://www.gov.br/cvm/pt-br/assuntos/noticias` has static article links under `/noticias/{year}/` |
| B3 | No feed or JSON endpoint verified. `https://www.b3.com.br/pt_br/noticias/` has static article links (9 on the page). The ofícios page `https://www.b3.com.br/pt_br/regulacao/oficios-e-comunicados/oficios-e-comunicados/` is a Lumis portal form (POST-driven) | HTML only | Could not verify bulk access; `sitemap.xml` redirects to an error page; robots.txt is effectively empty (13 bytes) |
| Tesouro Nacional | `https://www.gov.br/tesouronacional/++api++/pt-br/noticias/@search?portal_type=News%20Item&sort_on=effective&sort_order=descending&b_size=N` | Plone REST JSON: `@id`, `title`, `description`, `effective`. Adding `&fullobjects=1` returns the body as Volto `blocks` | `items_total` = 233, pageable with `b_start`. No RSS (the `/RSS` and `@@search_rss` paths are 404) |
| Ministério da Fazenda | `https://www.gov.br/fazenda/pt-br/@@search_rss?portal_type:list=News%20Item&sort_on=Date&sort_order=reverse` | RSS 1.0, 15 newest mixed items (filter on `/assuntos/noticias/`), no body. REST `@search` returns 401 | HTML listing `https://www.gov.br/fazenda/pt-br/assuntos/noticias?b_start:int=30` works (returned Sept 2026 items); URLs follow `/noticias/{year}/{month}/slug` |
| IBGE | API: `https://servicodados.ibge.gov.br/api/v3/noticias/?qtd=N&page=P`; date filter `&de=MM-DD-YYYY&ate=MM-DD-YYYY` worked. RSS: `https://agenciadenoticias.ibge.gov.br/agencia-rss` | API JSON: `id, tipo, titulo, introducao, data_publicacao, produtos, editorias, link` (intro only, no body). RSS: 10 items with full HTML body (avg 42k chars raw) | API count = 6,159 items back to 16 Jan 2004 |
| IPEA | `https://www.ipea.gov.br/portal/categorias/45-todas-as-noticias/noticias?format=feed&type=rss` | RSS, 10 items, summary about 1.1k chars, category | Feed spans about 5 weeks; no archive endpoint verified |
| ANBIMA | No feed found (`/pt_br/noticias/rss.xml` is 404; the listing loads via JS `lumgetdata/list.json`). `https://www.anbima.com.br/sitemap.xml` works | Sitemap: 50,000 `<loc>` entries (exactly the protocol cap, so possibly truncated), of which 20,629 under `/pt_br/noticias/` or `/pt_br/imprensa/`; no `lastmod` | URL list only; dates must come from the article pages. robots.txt is 404 |

Licences I saw on the pages:
- gov.br (Fazenda and CVM pages): footer says "Todo o conteúdo deste site está publicado sob a licença Creative Commons Atribuição-SemDerivações 3..." and links `creativecommons.org/licenses/by-nd/3.0/deed.pt`.
- EBC portal terms (`https://www.ebc.com.br/termos-de-uso-e-condicoes-gerais-do-portal-da-ebc`): "Reprodução autorizada mediante indicação da fonte." and "O Usuário deverá utilizar o Portal apenas para finalidades de uso pessoal, sem intuito comercial ou lucrativo."
- I did not find a Creative Commons link on the Agência Brasil pages I fetched.
- BCB, IBGE, IPEA licence pages: not checked.

## 3. Historical backfill

### Sitemaps (all verified unless noted)

| Outlet | Sitemap | Structure and depth |
|---|---|---|
| Valor | `https://valor.globo.com/sitemap/valor/sitemap.xml`; news: `.../sitemap/valor/news.xml` | Index of 5,540 daily files `.../valor/YYYY/MM/DD_1.xml`, from 2011-07-25. I checked 2015/03/10: 224 URLs. News sitemap: 720 URLs over about 2.5 days, with titles and publication dates |
| G1 | `https://g1.globo.com/sitemap/g1/sitemap.xml` | 9,139 daily files from 2003-01-05; all sections (filter on `/economia/`) |
| O Globo | `https://oglobo.globo.com/sitemap/oglobo/sitemap.xml`; news: `.../oglobo/news.xml` | 1,751 daily files; oldest 2010-04-15 |
| Pipeline | `https://pipelinevalor.globo.com/sitemap/pipelinevalor/sitemap.xml` | 1,815 daily files from 2021-02-02 |
| Valor Investe | `https://valorinveste.globo.com/sitemap/valorinveste/sitemap.xml` | Listed in robots.txt; not fetched |
| Estadão | `https://www.estadao.com.br/arc/outboundfeeds/sitemap-index-by-day/?outputType=xml` | 9,943 daily entries `.../sitemap/YYYY-MM-DD/?outputType=xml` back to 1999-07-20. I checked 2015-03-10: 391 URLs, all sections |
| Folha | `https://www1.folha.uol.com.br/sitemap.xml` | Index of 28 per-section sitemaps. `.../mercado/sitemap.xml` is a news sitemap only (44 URLs, 2 days). No deep archive via sitemap verified |
| InfoMoney | `https://www.infomoney.com.br/sitemap_index.xml`; `.../news-sitemap.xml` | 678 sub-sitemaps (`post-sitemapN.xml`), lastmod from 2001. News sitemap: 709 URLs over 2 days |
| Exame | `https://exame.com/sitemap.xml` | Index of 21 section sitemaps; depth not verified (`/economia/sitemap.xml` returned 404) |
| Bloomberg Línea | `https://www.bloomberglinea.com.br/arc/outboundfeeds/full-sitemap-index/?outputType=xml` | 1,912 daily entries from 2021-07-15; news sitemap `.../news-sitemap.xml/?outputType=xml` |
| Brazil Journal | `https://braziljournal.com/sitemap.xml` | Monthly `sitemap-posttype-post.YYYYMM.xml`, 157 entries, lastmod from Oct 2014 |
| NeoFeed | `https://neofeed.com.br/sitemap_index.xml` | 32 per-category sitemaps, lastmod from 2020-06 |
| Money Times | `https://www.moneytimes.com.br/sitemap_index.xml`; `.../news-sitemap.xml` | 140 sub-sitemaps, lastmod from Nov 2017 |
| Seu Dinheiro | `https://www.seudinheiro.com/sitemap_index.xml` | 84 sub-sitemaps, from Apr 2019 |
| Suno | `https://www.suno.com.br/noticias/sitemap_index.xml` | 76 sub-sitemaps, from Feb 2019 |
| InvestNews | `https://investnews.com.br/sitemap_index.xml`; `.../sitemap-news.xml` | 32 sub-sitemaps; oldest lastmod Feb 2024 |
| CNN Brasil | `https://www.cnnbrasil.com.br/sitemap_index.xml`; `.../sitemap-news.xml` | 533 numbered files, not date-keyed; news sitemap has 500 URLs over about 1.5 days |
| UOL Economia | `https://economia.uol.com.br/sitemap/v2/index.xml`; `.../v2/news-01.xml` | Monthly `YYYYMM.xml` from 201902 (94 entries); news sitemap has 411 URLs over 3 days |
| Agência Brasil | `https://agenciabrasil.ebc.com.br/sitemap.xml` | 16 pages (`?page=N`); contents not inspected |
| Brazilian Report | `https://newsletters.brazilian.report/sitemap.xml` | 7,649 URLs, lastmod from Dec 2024, with news tags |
| Poder360 | `https://www.poder360.com.br/sitemap_index.xml` | Could not verify (timed out twice) |
| Reuters | Sitemap endpoints return 200, but robots.txt disallows everything | Do not use |

### WordPress REST API

Verified with `GET /wp-json/wp/v2/posts`: JSON with `date`, `link`, `title`, `content.rendered`, `excerpt`, `categories`, `tags`. The `before=` / `after=` date filters work, and the `X-WP-Total` header gives the count.

| Site | Posts, all time | Posts before 2019 |
|---|---|---|
| InfoMoney | 579,463 | 386,880 |
| Money Times | 258,638 | 25,846 |
| Poder360 | 220,469 | 16,605 |
| Seu Dinheiro | 69,233 | 1,800 |
| Suno Notícias (`/noticias/wp-json/...`) | 63,103 | 1,453 |
| Brazil Journal | 11,742 | 2,061 |

Do not use it on NeoFeed, CNN Brasil or InvestNews: their robots.txt lists `Disallow: /wp-json` (InvestNews also returns 403).

### Free datasets

- **GDELT: not verified live.** The DOC API returned 429 on all three attempts, with the message "Please limit requests to one every 5 seconds or contact ... for larger queries. All high-traffic users should switch to our ngrams dataset". The rest is from memory, so confirm before designing around it:
  - DOC 2.0 API (`https://api.gdeltproject.org/api/v2/doc/doc?query=...&mode=artlist&format=json`) returns, per article, URL, title, seen date, domain, language, source country and social image; at most 250 records per query.
  - Filters include `domain:`, `sourcelang:portuguese`, `sourcecountry:BR`. Tone comes as aggregate timelines or as a filter.
  - The GKG 2.0 files (15-minute CSVs, from Feb 2015, also in BigQuery) carry the URL, themes, persons, organisations, locations and tone; the page title sits in the Extras field. Portuguese is covered through the translingual stream.
  - No full text and no summary anywhere. Timestamps are when GDELT saw the article, not publication time.
- **Common Crawl CC-NEWS.** Verified: the index page `https://data.commoncrawl.org/crawl-data/CC-NEWS/index.html` says "WARC files are released on a daily basis. The news crawl was started in 2016", and `.../CC-NEWS/2026/09/warc.paths.gz` exists.
  - From memory: files are about 1 GB WARCs with full HTML and no per-domain index, so you download and filter. Which Brazilian outlets are included is not verified.
  - Limit (verified in robots.txt): Valor, O Globo, Pipeline, Valor Investe, Estadão, Folha, UOL, Suno and InvestNews all `Disallow: /` for `CCBot`, so expect them absent for recent years.
- **Common Crawl main index.** Verified: `https://index.commoncrawl.org/collinfo.json` lists 128 crawls, latest `CC-MAIN-2026-39`. A per-domain URL lookup via the CDX API is possible; the same CCBot exclusions apply.
- **Not tested:** Wayback Machine CDX API, Media Cloud.

## 4. Access rules

### robots.txt (all fetched)

| Host | Rules for `User-agent: *` | Bots named with `Disallow: /` |
|---|---|---|
| valor / pipelinevalor / valorinveste (.globo.com) | `Disallow: /busca/`, `Disallow: /beta/` | GPTBot, ClaudeBot, anthropic-ai, CCBot, Google-Extended, PerplexityBot, Bytespider, ChatGPT-User, cohere-ai, FacebookBot, Omgilibot, OAI-SearchBot, Grok*, Copilot*, SabiáBot, "AI-Powered-Bot" |
| oglobo.globo.com | 40 path rules, none on news or rss | Same Globo list plus AI2Bot and cohere-training-data-crawler |
| g1.globo.com | 14 path rules (`/busca/*` etc.) | None |
| www.estadao.com.br | 22 rules including `Disallow: /feed/$` (the `/arc/outboundfeeds/` paths are not disallowed) | GPTBot, ClaudeBot, anthropic-ai, CCBot, PerplexityBot, Bytespider, ChatGPT-User, OAI-SearchBot, PetalBot; `Google-Extended: Disallow: /opiniao/*` |
| www1.folha / feeds.folha (same file) | 7 path rules | 177 user-agent lines, including ClaudeBot, Claude-User, Claude-Web, Claude-SearchBot, **Claude-Code**, anthropic-ai, CCBot, PerplexityBot, Amazonbot, Diffbot, DeepSeekBot, meta-external* |
| economia.uol.com.br / www.uol.com.br | 6 path rules | Similar long AI list (ClaudeBot, Claude-User, Claude-Web, CCBot, Google-Extended, DeepSeek, Diffbot and others) |
| www.reuters.com | **`Allow: /plus/` then `Disallow: /`**; only whitelisted bots allowed | Everyone not whitelisted |
| br.investing.com | Many path rules (`/research/`, `/pro/`, ...); `/rss/` and `/news/` are not disallowed. Returned 200 to one UA and 403 to curl and Chrome UAs | Not inspected beyond the `*` group |
| www.suno.com.br | `Allow: /`; `Disallow: /wp-admin/`, `/pipefy/` | GPTBot, ClaudeBot, anthropic-ai, CCBot, Google-Extended, Amazonbot, Applebot-Extended, Bytespider, meta-externalagent, Googlebot-News |
| investnews.com.br | `Disallow: /wp-json/`, `/inv-api/`, `/?s=` | GPTBot, ClaudeBot, CCBot, Google-Extended, Amazonbot, Applebot-Extended, Bytespider, Meta-ExternalAgent |
| neofeed.com.br | `Disallow: /wp-json`, `/categoria/*`, `/?p=*`, `/en/`, `/es/`, `/zh/`; `ClaudeBot: Crawl-delay: 10` | None |
| www.cnnbrasil.com.br | `Disallow: /wp-json/`, `/author/`, `/*?s=` and others | None |
| www.infomoney.com.br | `Disallow:` (empty, so everything is allowed) | None |
| exame.com | `Disallow: /wp-admin/`, `/preview/`, `/busca/`, `/informe-publicitario/`; `Google-Extended: Allow: /` | None |
| www.bloomberglinea.com.br | `Allow: /`; disallows `/pf/api/v3/*`, search, account | None |
| braziljournal.com | `Crawl-delay: 1`; disallows wp-admin and plugins | None |
| moneytimes / seudinheiro / poder360 / brazilian.report | Only admin and search paths | None |
| agenciabrasil.ebc.com.br | `Crawl-delay: 10`; Drupal system paths | None |
| dados.cvm.gov.br | `Disallow: /api/`, `/revision/`, `/dataset/*/history`; `Crawl-delay: 10` | None |
| bcb / gov.br / IBGE / IPEA | System paths only | None |

A custom collector UA matches only the `*` group. Technically that leaves feeds and articles open everywhere except Reuters, but the named-bot lists show these publishers' stance on AI use.

### Terms of use (quotes from pages I fetched)

- **Globo (Valor, O Globo; identical text at `https://valor.globo.com/termos-de-uso/` and `https://oglobo.globo.com/termos-de-uso/`): explicit prohibition.**
  - "Nos Produtos Digitais é proibida a utilização, de aplicativos spider, ou de mineração de dados, de qualquer tipo ou espécie, além de outro aqui não tipificado, mas que atue de modo automatizado..."
  - "Acessar ou coletar dados dos Produtos Digitais através de meios automatizados e fazer uso de ferramentas de data mining, coleta de dados ou extração de dados, sem prévia autorização da EDITORA" (listed among prohibited acts).
  - "Não é permitido que o conteúdo produzido pela EDITORA ... seja utilizado para (i) desenvolvimento de qualquer tipo de software, inclusive sistema de inteligência artificial generativa; (ii) treinamento de aprendizado automático (machine learning)...; (iii) reprodução do Conteúdo ou geração de conteúdo a partir do Conteúdo por sistema de inteligência artificial para disponibilização em qualquer meio."
  - Pipeline and Valor Investe terms were not fetched separately. The G1 terms page (`https://g1.globo.com/institucional/termos-de-uso-do-g1.ghtml`) was fetched, but my keyword scan found no matching clause and I did not read it in full.
- **Estadão (`https://www.estadao.com.br/termo-de-uso`): personal use allowed, AI/software use excluded.** "O uso pessoal e não comercial aqui tratado não abrange a utilização dos Conteúdos, seja por meios manuais ou automatizados, para fins de desenvolvimento ou aprimoramento de softwares, abrangendo, dentre outros, o treinamento em aprendizagem de máquina e de inteligências artificiais."
- **Exame (`https://exame.com/institucional/termos-de-uso/`): explicit prohibition, with a personal-use carve-out.**
  - "É proibido usar, reproduzir, ... fazer scrapping ou engenharia reversa das Plataformas e do Conteúdo para qualquer finalidade, sem o consentimento prévio e expresso da exame."
  - "acessar ou coletar dados de maneira automatizada sem a prévia autorização da exame" (prohibited).
  - But also: "Você pode acessar e utilizar nossas Plataformas, bem como baixar ou copiar nosso Conteúdo ... exclusivamente para seu uso pessoal" and "Copiar, armazenar ou divulgar o Conteúdo para qualquer outra finalidade que não para uso pessoal é estritamente proibido".
- **Brazil Journal (`https://braziljournal.com/termos-de-uso/`): explicit prohibition.** "É proibido usar, reproduzir, modificar, traduzir, publicar, transmitir, distribuir, executar, exibir, licenciar, vender, fazer scrapping ou engenharia reversa do site e do conteúdo para qualquer finalidade, sem o consentimento prévio e expresso do Brazil Journal."
- **NeoFeed (`https://neofeed.com.br/termos-de-uso/`): explicit prohibition.** "Não utilizar softwares para automatização de acessos no SITE, com exceção das bibliotecas disponibilizadas pelo NEOFEED, bem como softwares de mineração ou coleta automatizada de dados, de qualquer tipo ou espécie..."
- **Money Times and Seu Dinheiro (same template; `/termos-de-uso/` on each):** no clause on automated access matched. On copying: "não poderá ser copiado, distribuído, modificado, reproduzido, publicado ou utilizado no todo ou em parte, salvo para os fins autorizados ou aprovados ... previamente e por escrito."
- **Suno (`https://www.suno.com.br/termos-de-uso/`):** "O conteúdo deste website não pode ser reproduzido e/ou distribuído sem a expressa autorização da Suno." No automated-access clause matched.
- **CNN Brasil (`https://conteudos.cnnbrasil.com.br/termos-de-uso-da-cnn-brasil/`):** "É proibida a reprodução, divulgação e distribuição, total ou parcial, de todos os conteúdos publicados nas plataformas da CNN Brasil, exceto sob prévia e expressa autorização." Nothing on automated access matched.
- **Poder360 (`https://www.poder360.com.br/termos-de-uso/`):** fetched; no clause on automated access or reproduction by readers matched my keyword scan.
- **Reuters:** the ToS page returned 401. The robots.txt header (verified) reads: "Collection of content, data and/or information from reuters.com through automated means is prohibited unless you have prior written consent from Reuters and may only be conducted for the purposes explicitly described in such permission."
- **Could not check:** InfoMoney (`/termos-de-uso/` redirects to the privacy policy), Folha (no ToS link found; one page footer reads "É proibida a reprodução do conteúdo desta página em qualquer meio de comunicação, eletrônico ou impresso, sem autorização escrita da Folhapress"), UOL, Bloomberg Línea, InvestNews, Investing.com, Brazilian Report.

All keyword-scan results above ("no clause matched") mean I searched the page text for terms like robô, automatizado, scraping, mineração, inteligência artificial and reprodução; I did not read those documents end to end.

### Blocking and JavaScript

- **Investing.com Brasil:** article pages need JavaScript (Cloudflare challenge, 403 to curl); RSS is open.
- **Reuters:** 401 to curl on section and ToS pages.
- **E-Investidor old WordPress host:** Akamai returned 403/503 depending on UA.
- **InvestNews:** `/wp-json` returns 403 (Cloudflare page).
- **IBGE:** `agencia-noticias.feed?type=rss` returned a Cloudflare challenge; the `agencia-rss` URL and the API work.
- **JS-rendered listings:** B3 and ANBIMA (Lumis portal); Tesouro Nacional (Volto, but the `++api++` JSON works).
- Everything else returned full HTML to plain curl from the residential IP. Datacenter-IP behaviour is untested.

## 5. Recommended tiering

The "full text" tier needs your call. Several outlets put full text in their own feeds but forbid automated collection in their terms, and no commercial outlet I checked explicitly permits it. I kept those two cases apart.

**(a) Collect by feed: title, summary, link and metadata**
- Folha Mercado, UOL Economia, NeoFeed, IPEA, Exame `/feed/` (teaser). These feeds only carry summaries.
- Valor, Valor Investe, Pipeline, O Globo Economia, Estadão Economia, E-Investidor. The feeds carry long bodies, but the Globo and Estadão terms explicitly forbid automated collection or AI use. Store title, a truncated summary, link, date and category, and drop the body unless you accept the terms risk.
- Brazil Journal and Exame Invest. Full text is in the feed, but both terms explicitly forbid scraping; Exame's allows copying "exclusivamente para seu uso pessoal".
- CVM, Fazenda and Tesouro news listings (metadata only by nature).
- Investing.com RSS is title and link only anyway.

**(b) Full text**
- *Explicitly permitted or public:*
  - BCB: feeds plus the Copom JSON (complete history).
  - IBGE: RSS full text plus the API for the 2004-onward index.
  - gov.br (Fazenda, CVM news, Tesouro): CC BY-ND 3.0 footer.
  - CVM IPE dataset: ODbL.
  - Agência Brasil: EBC terms say "Reprodução autorizada mediante indicação da fonte".
- *Full text offered in the publisher's own feed, with no anti-automation clause found:* InfoMoney (terms not found), Money Times, Seu Dinheiro, Suno, Poder360, CNN Brasil, G1 Economia (terms not read in full), Bloomberg Línea and InvestNews (terms not checked).
  - Caveats: these terms still restrict reproduction and distribution, which matches your no-redistribution use. Suno and InvestNews block AI crawlers by name in robots.txt, which matters for the phase 2 RAG.
  - For backfill, the WordPress REST API is open on InfoMoney, Money Times, Seu Dinheiro, Suno and Poder360 (also Brazil Journal, subject to its terms).

**(c) Skip or link-only**
- Reuters: robots.txt disallows all, automated collection is expressly prohibited, 401 to curl, no feed.
- Investing.com article pages: JS challenge. Keep RSS titles and links only.
- The Brazilian Report: no feed; subscription newsletter. Sitemap URLs only if wanted.
- B3 and ANBIMA: no feed. ANBIMA is link-only via sitemap; B3 needs HTML scraping of the news page, and the ofícios need form POSTs that I did not work out.
- Article-page scraping of Folha, Valor, O Globo, Estadão and UOL: paywalled or metered, and the terms prohibit it. Use their sitemaps for URL, date and title backfill only (Valor from 2011, Estadão from 1999, G1 from 2003, O Globo from 2010, UOL Economia from 2019).

**Design notes that follow from the tests**
- Parse dates per source: Folha, UOL (Portuguese day and month names), Exame and Investing (no timezone), gov.br (`2026/10/07 18:08:01 GMT-3`).
- Unwrap Folha `redir.folha.com.br/...*` links.
- Handle ISO-8859-1 (UOL, Folha, CVM CSV).
- Dedupe by canonical URL: `pox.globo.com` and site feeds are duplicates, and UOL republishes Estadão Conteúdo.
- Filter all-section feeds (CNN, Poder360, G1, Valor main) by category or URL path.
- Validate that each response is actually XML or JSON, given the WAF behaviour described at the top.
