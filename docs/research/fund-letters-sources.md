<!-- Research report produced on 2026-10-08 to design the fund-letter collectors. Evidence labels are explained in the text. -->

# Brazilian manager letters: where and how to collect (checked 2026-10-08)

## Bottom line

- **No regulator source has the letters.** CVM open data has no letter-like document for ordinary funds (FI/FIF). It does index monthly management reports for FII/FIAGRO/FIDC, with direct PDF links.
- **The managers' own sites are the primary source.** Most are WordPress, and about 40 of the 50 WordPress sites I tested expose the media REST endpoint, which lists every PDF with its upload date. That is your generic collector.
- **Aggregators are useful for backfill only.** Empiricus (about 1,950 letters, Apr 2021 to Oct 2024, PDFs hosted) and Gorila (641 posts, PDFs hosted) are the two crawlable ones.
- **CVM's `cad_adm_cart_pj.csv` gives you the crawl frontier:** 1,551 active managers, 1,540 with a website field.

How to read the evidence labels: **opened** means I fetched the page or file myself (curl or WebFetch); **snippet** means a search result only; **could not verify** means what it says. PDF counts are links in the static HTML of one page load, so paginated or JS listings are undercounted. Platform labels are heuristic (string matches in the HTML). Frequency is inferred from filenames and titles, not from a stated policy.

Things I did that you should know about:
- I called `dados.cvm.gov.br/api/3/action/package_list` once before reading its robots.txt, which disallows `/api/`. Use the `/dados/` directory listings instead.
- Acionista answered one REST call, then served a Cloudflare challenge. I stopped there.
- I fetched one Bom Dia Mercado page before seeing that its robots.txt disallows ClaudeBot and other AI agents.
- I downloaded 5 sample PDFs and a few CVM files into the session scratchpad only. Nothing in the repo was touched.

## 1. Aggregators

| Site | Size / range | Hosted or linked | How to crawl | Robots / terms | Evidence |
|---|---|---|---|---|---|
| **Empiricus** `https://www.empiricus.com.br/conteudo-extra/cartas-dos-gestores/` | 131 listing pages × ~15 items ≈ 1,950 letters. Oldest on last page: April 2021. Newest on page 1: October 2024, so it looks discontinued. | Hosted. Each item is an HTML page linking a PDF, e.g. `https://www.empiricus.com.br/uploads/2024/09/Squadra_Carta_1S24.pdf` | Plain HTML pagination `/conteudo-extra/cartas-dos-gestores/page/{n}/`. Slugs encode manager, fund and period (`squadra-squadra-long-bias-carta-do-gestor-agosto-2024`). Pages carry `article:published_time`. Sitemap index at `/sitemap_index.xml`. | robots: `Disallow: /wp-admin/`, `/wp-json/`, `/util/`, `/app/`, `/?s=`, `/?buscar=`. The letters path is not disallowed; the REST API is. Terms: "Não modificar, copiar, compilar, realizar engenharia reversa, distribuir, transmitir, reproduzir, publicar" and "Não burlar qualquer tecnologia usada pela EMPIRICUS RESEARCH". | opened (pages 1, 2, 131, one item, robots; terms via WebFetch summary) |
| **Gorila** `https://gorila.com.br/blog/category/carta-do-gestor` | 641 posts in the category. Per-manager posts run to about Mar/Apr 2024; 2026 posts are monthly roundups ("Cartas das Gestoras (Setembro 2026)", "AI Summary"). | Hosted. Short post plus PDF, e.g. `https://gorila.com.br/wp-content/uploads/Absolute-Vertex_Cartas_Janeiro.23.pdf` | WordPress REST answered: `/wp-json/wp/v2/categories?search=carta` returned `{"id":510,"count":641,"slug":"carta-do-gestor"}`, so `/wp-json/wp/v2/posts?categories=510` should page through it (not run). Also `https://gorila.com.br/blog/sitemap.xml`. | robots: `User-agent: * / Allow: /`. Terms not checked. | opened |
| **Acionista** `https://acionista.com.br/category/noticias-do-mercado/carta-ao-investidor/` | Category count 302 (one REST call). Search results show letter posts from 2020 through July 2026, so it is active. | Hosted. Post plus PDF, e.g. `https://acionista.com.br/wp-content/uploads/2026/08/202607_RelatorioMensal_IbiunaHedgeSTHFIFCIC.pdf` (snippet). | Category page returned 200 to curl. REST worked once, then Cloudflare managed challenge (403). Plan for HTML pagination at a low rate. | robots.txt is 200 with an empty body. Terms not checked. | category count opened; PDF URL and date range from snippets |
| **Bom Dia Mercado** `https://www.bomdiamercado.com.br/` | Unknown. | Summary plus hosted PDF, e.g. `https://www.bomdiamercado.com.br/wp-content/uploads/2024/02/02-24-Genoa-Capital.pdf`; has subscription prompts. | Listing URL not found (`/carta-do-gestor/` is 404; posts live under `/carta-do-gestor/<slug>/` and `/diversos/<slug>/`). | robots: `User-agent: * Allow: /`, but `Disallow: /` for GPTBot, Google-Extended, Applebot-Extended, ClaudeBot, anthropic-ai, Meta-ExternalAgent, CCBot. A clear "no AI use" signal that matters for phase 2. | one post and robots opened |
| **BTG "Cartas dos gestores"** `https://investimentos.btgpactual.com/cartas-dos-gestores` (orama.com.br/cartas-dos-gestores redirects here) | Unknown. | Unknown. | Returns a JS shell with no content in static HTML. Needs a headless browser or the backing API. | Could not verify. | could not verify contents |
| **XP** `conteudos.xpi.com.br` | Monthly compiled "opinião de centenas de gestores" plus some hosted PDFs. | Mixed. | 403 to my client. | Could not verify. | snippet only |

Checked and not aggregators: `buysidebrazil.com` (macro research), `gestoras.com.br` (unrelated), `maisretorno.com` (fund and manager directory, no letters found). The Viagem Lenta blog describes a reader-shared Amazon Drive folder (since Aug 2019) behind a newsletter sign-up; no public URL. `carteiraperfeita.com.br/gestora/<slug>/` is a manager directory with a REST type `gestora`; I did not confirm it carries letters.

I found no paid aggregator with a public catalogue.

## 2. Official sources (CVM, Fundos.NET, ANBIMA)

All files below were opened. Format is CSV, `;` separated, Latin-1. `dados.cvm.gov.br/robots.txt` says `Disallow: /api/` and `Crawl-Delay: 10`.

**Manager registry (your seed universe)**
- Dataset page: `https://dados.cvm.gov.br/dataset/adm_cart-cad` (updated daily).
- Data: `https://dados.cvm.gov.br/dados/ADM_CART/CAD/DADOS/cad_adm_cart.zip`, containing `cad_adm_cart_pj.csv`, `cad_adm_cart_pf.csv`, `cad_adm_cart_diretor.csv`, `cad_adm_cart_resp.csv`, `cad_adm_cart_socios.csv`.
- Dictionary: `https://dados.cvm.gov.br/dados/ADM_CART/CAD/META/meta_cad_adm_cart.zip`.
- `cad_adm_cart_pj.csv` columns: `CNPJ;DENOM_SOCIAL;DENOM_COMERC;DT_REG;DT_CANCEL;MOTIVO_CANCEL;SIT;DT_INI_SIT;CATEG_REG;SUBCATEG_REG;CONTROLE_ACIONARIO;TP_ENDER;LOGRADOURO;COMPL;BAIRRO;MUN;UF;CEP;DDD;TEL;VL_PATRIM_LIQ;DT_PATRIM_LIQ;EMAIL;SITE_ADMIN`.
- 3,058 rows; 1,551 with `SIT = EM FUNCIONAMENTO NORMAL`; 1,540 of those have `SITE_ADMIN`.
- `SITE_ADMIN` is dirty: mixed schemes, some stale. Examples: Studio is listed as `studioinvest.com.br` but the live site is `studioinvestimentos.com.br`; Meta as `metaam.com.br` (redirects to `metaasset.com.br`); Equitas redirects to `dryscapital.com.br`.

**Fund registry**
- `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv`. Has `CNPJ_FUNDO`, `DENOM_SOCIAL`, `SIT`, `CLASSE`, `CNPJ_ADMIN`, `ADMIN`, `PF_PJ_GESTOR`, `CPF_CNPJ_GESTOR`, `GESTOR`, `CLASSE_ANBIMA` and others. No website field.
- `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip` (post-Resolução 175 model): `registro_fundo.csv` (includes `CPF_CNPJ_Gestor`, `Gestor`), `registro_classe.csv`, `registro_subclasse.csv`. Use it to join fund to manager by CNPJ.
- Also there: `cad_fi_hist.zip`.

**Per-fund documents: what exists and what is letter-like**

| Dataset | Files | Letter-like? |
|---|---|---|
| `fi-doc-lamina` | `https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/lamina_fi_YYYYMM.zip` (2019-01 onward, plus `HIST/`). Each zip has `lamina_fi_`, `lamina_fi_carteira_`, `lamina_fi_rentab_ano_`, `lamina_fi_rentab_mes_` CSVs. | No. Structured fields only (objective, policy, fees, returns). No manager commentary and no PDF. |
| `fi-doc-eventual` | `https://dados.cvm.gov.br/dados/FI/DOC/EVENTUAL/DADOS/eventual_fi_YYYY.csv` (2005 to 2026). Columns `TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;DENOM_SOCIAL;ID_SUBCLASSE;DT_COMPTC;DT_RECEB;TP_DOC;NM_ARQ;ID_DOC;LINK_ARQ;RESULTADO_AUDITORIA`. | Partly. It is a bulk index with a direct link per document. |
| `fi-doc-perfil_mensal` | `.../FI/DOC/PERFIL_MENSAL/DADOS/perfil_mensal_fi_YYYYMM.csv` | No (structured). |
| `fi-doc-extrato` | `.../FI/DOC/EXTRATO/DADOS/extrato_fi.csv`, `extrato_fi_YYYY.csv` | No (structured). |
| `fi-doc-entrega` | `.../FI/DOC/ENTREGA/DADOS/fi_entrega_documento_YYYYMM.zip` | No. Delivery log only (type, period, timestamp). |
| `fi-doc-compl` | `.../FI/DOC/COMPL/DADOS/compl_fi_YYYYMM.zip` (2018-01 to 2019-04 only) | No. |

What `eventual_fi_2026.csv` actually contains (114,985 rows):
- For ordinary funds (`FI`, `CLASSES - FIF`) the only document types are `REGUL FDO`, `SGF ANEXO`, `DEMONST CONTAB`, `FATO RELEV`, `AGO`, `SGF APENDICE`, `EDITAL AGO`, `PROSPEC.DISTRIB`. There are no letters or management reports.
- `RELAT GERENCIAL` has 3,390 rows: FII 2,947, FIAGRO 375, FIDC 68. Links look like `https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento?id=1122708`.
- I fetched that sample link: 200, `application/pdf`, 2.09 MB, body starts with `%PDF`. No login. `fnet.bmfbovespa.com.br/robots.txt` is 404.
- Other link hosts in the file: `web.cvm.gov.br/app/fundosweb/...` (regulations and annexes) and `sistemas.cvm.gov.br/docsrecebidos/`.

So if "relatórios de gestão" in your scope includes listed-fund monthly reports, this one CSV per year is a complete, bulk, official index with metadata (CNPJ, reference date, receipt date). For multimercado and equity fund letters, CVM has nothing.

**Fundos.NET / CVMWeb search UI:** I did not test the interactive query endpoints. The open-data `eventual` file already carries the Fundos.NET document ids.

**ANBIMA:** The developers page (`https://developers.anbima.com.br/pt/documentacao/fundos-v2-rcvm-175/visao-geral/`, opened) lists Feed Fundos v2 endpoints: Lista, Detalhes, Detalhes-histórico, Série histórica, Fundos instituição, Segmento do Investidor, Notas explicativas, Lote. It mentions no letters or documents. Access terms and pricing: could not verify. Snippets say ANBIMA Data covers registration data, fees, series and regulations. Not a letter source.

## 3. Seed list of managers

"REST open" means `https://<host>/wp-json/wp/v2/media?mime_type=application/pdf` returned 200 with a total count (shown) to an unauthenticated request.

### A. Opened, letters found, plain HTML or open API (47)

| # | Manager | Listing URL | Format | Freq. (inferred) | Listing mechanics | Example |
|---|---|---|---|---|---|---|
| 1 | Verde | `https://www.verdeasset.com.br/#/performance` | PDF | monthly, back to 1998 | JS single-page app, but backed by public JSON: fund list at `/public/fundos/data/lista_preview.json` (16 funds), per-fund index at `/public/fundos/data/relatorios/{id}.json` with `{ano, mes, url}` | `https://www.verdeasset.com.br/public/files/rel_gestao/158094/Verde-REL-2026_08.pdf` (200, PDF) |
| 2 | Kapitalo | `https://www.kapitalo.com.br/cartas-do-gestor` | PDF | monthly, plus thematic | Hub links to per-family pages `/carta-do-gestor/{kapa-e-zeta,k10,nw3,tarkus,cartas-tematicas}` with a `?ano=YYYY` filter; 92 PDFs on one page. REST open (1,431) | `https://www.kapitalo.com.br/wp-content/uploads/2020/02/kapitalo_cartamensal_KAPPA-ZETA_novembro-24-1.pdf` |
| 3 | Dynamo | `https://www.dynamo.com.br/2436-2/` | HTML post per letter plus PDF | about quarterly (128 letters) | Only 10 PDFs in the first load; older ones via per-letter posts (`/carta-128-os-serendipitistas/`) or the API. REST open (347) | `https://www.dynamo.com.br/wp-content/uploads/2026/04/Carta-Dynamo-128.pdf` |
| 4 | Squadra | `https://www.squadrainvest.com.br/cartas/` | PDF | annual / semi-annual | Plain links, 21 PDFs. REST open (137) | `https://www.squadrainvest.com.br/wp-content/uploads/2022/09/carta-2011.pdf` |
| 5 | Atmos | `https://www.atmoscapital.com.br/cartas.php` | PDF | irregular | Plain links, 33 PDFs, numbered | `https://www.atmoscapital.com.br/documentos/cartas/Carta-Atmos-01.pdf` |
| 6 | IP Capital | `https://ip-capitalpartners.com/relatorios/` | PDF plus HTML landing pages (`/reports/<slug>/`) | irregular | Elementor. REST open (143) | `https://ip-capitalpartners.com/wp-content/uploads/2024/01/IP_RG_obrigadocharlie.pdf` |
| 7 | Legacy | `https://www.legacycapital.com.br/cartas-e-calls-mensais/` (credit: `/cartas-e-calls-de-credito/`) | PDF | monthly | Items load via `wp-admin/admin-ajax.php?action=filter_items&term=`; static HTML shows only policy PDFs. REST open (457) | `https://www.legacycapital.com.br/wp-content/uploads/202609_Carta-Mensal.pdf` |
| 8 | Adam | `https://adamcapital.com.br/documentos/relatorios-e-call/` | PDF | monthly | Plain. REST open (445) | `https://adamcapital.com.br/wp-content/uploads/2026/10/Relatorio-Gerencial-Adam-Advanced-II-Set-2026.pdf` |
| 9 | Bahia | `https://www.bahiaasset.com.br/carta-do-gestor/` | PDF | monthly | Plain, 73 PDFs. REST open (183; returns relative `source_url`) | `https://www.bahiaasset.com.br/wp-content/uploads/2026/10/carta_do_gestor_set26.pdf` |
| 10 | Kinea | `https://www.kinea.com.br/blog/categoria/carta-do-gestor/` (also `/blog/categoria/carta-trimestral-acoes/`) | HTML posts | monthly / quarterly | Listing is partly client-side templated. Use REST posts by category (page references `wp-json/wp/v2/categories/340`). robots disallows `/category/`, `/pdf/`, `/author/` | `https://www.kinea.com.br/blog/carta-do-gestor-highlander-guerreiro-imortal/` |
| 11 | Genoa | `https://www.genoacapital.com.br/relatorios.html` | PDF | monthly, plus semi-annual | Static HTML, 78 PDFs | `https://www.genoacapital.com.br/docs/CartaMensalGenoaCapital_Out25.pdf` |
| 12 | Vista | `https://vistacapital.com.br/relatorios/` | PDF | monthly, plus letters | Plain, 88 PDFs. REST open (1,225) | `https://vistacapital.com.br/wp-content/uploads/2025/06/Atribuicao-de-Performance-junho-17-vlgg-1.pdf` |
| 13 | Alaska | `https://www.alaska-asset.com.br/cartas/` | PDF | monthly / quarterly | Plain, 130 PDFs. REST closed (401) | `https://www.alaska-asset.com.br/pdf/cartas/2014/trimestre4.pdf` |
| 14 | Brasil Capital | `https://brasilcapital.com/cartas-de-gestao/` | PDF | quarterly | 8 PDFs visible; older ones probably paginated. REST closed (401) | `https://brasilcapital.com/wp-content/uploads/2024/10/Carta_BC_3T24.pdf` |
| 15 | Bogari | `https://bogaricapital.com.br/biblioteca/cartas/` | PDF | irregular | Plain, 37 PDFs. robots `Crawl-delay: 1`. REST open (287) | `http://bogaricapital.com.br/wp-content/uploads/2015/07/Carta-Bogari-29-Discurso-x-Pr%C3%A1tica.pdf` |
| 16 | JGP | `https://www.jgp.com.br/comunicacoes/carta/` and `/comunicacoes/relatorios/` | PDF | monthly | Plain, 336 PDFs. REST open (1,350) | `https://www.jgp.com.br/wp-content/uploads/2026/10/JGP_Relatorio-de-Gestao_Fundos-de-Multimercados_Set26.pdf` |
| 17 | Occam | `https://occambrasil.com.br/cartas-mensais/` and `/cartas-tematicas/` | PDF | monthly, plus thematic | Plain, 52 and 13 PDFs. REST answered (1,658 media) but the PDF filter returned an empty list | `https://occambrasil.com.br/wp-content/uploads/2019/08/Carta-01-Superapp.pdf` |
| 18 | Ace | `https://acecapital.com.br/cartas-multimercado/` and `/cartas-renda-variavel/` | PDF | monthly | Plain, 84 and 8 PDFs. REST open (490) | `https://acecapital.com.br/wp-content/uploads/Carta-Setembro-2026.pdf` |
| 19 | Opportunity | `https://www.opportunity.com.br/Home/Destaques` | HTML page plus PDF | monthly | Plain links `/Home/CartaGestor?pub=<url-encoded date>`; predictable PDF name | `https://www.opportunity.com.br/content/pdf/carta_gestor/CartaGestao_202609.pdf` |
| 20 | Organon | `https://organoncapital.com.br/cartas/` | PDF | semi-annual | Plain, 11 PDFs. REST open (80) | `https://organoncapital.com.br/wp-content/uploads/2026/07/Carta_01_1S21.pdf` |
| 21 | AZ Quest | `https://azquest.com.br/comunicacao-gestor.php` | PDF | monthly, plus quarterly | Custom PHP, plain, 111 PDFs under `/arquivos/carta/`. (`/carta-do-gestor/` returned an empty body to curl.) | `https://azquest.com.br/arquivos/carta/2021_06-AZ-Quest-Azimut-Equity-China-Carta-Mensal.pdf` |
| 22 | Leblon | `https://leblonequities.com.br/cartas/` (letters) and `/relatorios/` (monthly) | HTML posts; PDFs | letters stopped at no. 22 (Jan 2018); reports monthly | Plain. REST open (1,040) | `https://leblonequities.com.br/carta/carta-leblon-22/` |
| 23 | Navi | `https://www.navi.com.br/navi-capital/relatorios-mensais/` and `/navi-capital/cartas/` | PDF | monthly | Reports page plain (10 PDFs); letters page has 0 PDFs in static HTML, so it is JS-loaded. REST open (319) | `https://www.navi.com.br/wp-content/uploads/relatorionavils_set26.pdf` |
| 24 | Charles River | `https://charlesriver.com.br/conteudo/` | PDF | irregular; monthly performance report | Plain. REST open (95) | `https://charlesriver.com.br/wp-content/uploads/Relatorio-de-Desempenho-Setembro-2026.pdf` |
| 25 | HIX | `https://hixcapital.com.br/index.php/conteudo/` | PDF | semi-annual | 32 PDFs; some hrefs point at a bare IP (`http://54.226.246.61/wp-content/...`) and need the host rewritten. REST open (348) | `http://54.226.246.61/wp-content/uploads/2021/08/HIX-Capital-Carta-aos-Investidores-Dez-2016.pdf` |
| 26 | Studio | `https://studioinvestimentos.com.br/cartas-anuais/` (monthly: `/relatorios-de-desempenho/`) | PDF | annual | Plain, 23 PDFs | `https://studioinvestimentos.com.br/wp-content/uploads/2024/03/Studio-Investimentos-Carta-2023-.pdf` |
| 27 | Sparta | `https://www.sparta.com.br/cartas-mensais/` | PDF | monthly | Plain, 36 PDFs, predictable names | `https://www.sparta.com.br/uploads/CartaMensal_202301.pdf` |
| 28 | STK | `https://stkcapital.com.br/conteudo-comentarios-mensais/` | PDF | monthly | Plain, 40 PDFs. REST open (488) | `https://stkcapital.com.br/wp-content/uploads/2025/02/Comentario-Mensal-STK-Long-Biased-FIC-FIA-Janeiro.pdf` |
| 29 | Neo | `https://www.neo.com.br/cartas-de-gestao/` | HTML pages | monthly | Latest items in static HTML; the rest via JS widget. Predictable slugs | `https://www.neo.com.br/cartas-do-gestor-agosto-2026/` |
| 30 | Mar Asset | `https://www.marasset.com.br/conteudo-mar/` | PDF via redirect | irregular | `/document/<slug>/` redirects to a PDF under `/site/wp-content/uploads/` | `https://www.marasset.com.br/document/carta/` |
| 31 | Vokin | `https://www.vokin.com.br/cartas-mensais/` | HTML page plus PDF | monthly (no. 260 in Sep 2026) | Plain. REST open (279) | `https://www.vokin.com.br/wp-content/uploads/2026/10/Carta-Mensal-Vokin-Investimentos-SET2026-1.pdf` |
| 32 | Santa Fé | `https://santafe.com.br/relatorios/` | PDF | quarterly | Plain, 18 PDFs. REST open (198) | `https://santafe.com.br/wp-content/uploads/2022/04/2022_01_Tri.pdf` |
| 33 | Meta Asset | `https://metaasset.com.br/cartas-mensais/` | PDF | monthly | Plain, 69 PDFs. REST open (328) | `https://metaasset.com.br/wp-content/uploads/2026/10/Carta-Mensal-Setembro-2026.pdf` |
| 34 | 3R | `https://3r-invest.com.br/carta-do-gestor/` | PDF | monthly | Plain, 89 PDFs outside the media library (`/arquivos/...`) | `https://3r-invest.com.br/arquivos/cartas-do-gestor/2019/3R_Genus_Hedge_FIM-Carta_do_Gestor-Agosto_2019.pdf` |
| 35 | Journey | `https://journeycapital.com.br/publicacoes/relatorios/` | PDF | monthly | Plain, 12 PDFs. REST open (420) | `https://journeycapital.com.br/wp-content/uploads/2026/01/Carta_Mensal_DEZ-25_Asset.pdf` |
| 36 | Kadima | `https://www.kadimaasset.com.br/biblioteca/` | HTML page per letter | quarterly | Plain links `/cartas/<slug>/`. REST 403 | `https://www.kadimaasset.com.br/cartas/carta_06_2026/` |
| 37 | Exploritas | `https://www.exploritas.com.br/cartas-mensais/` | PDF via download script | monthly | Links are `wp-content/plugins/download-attachments/includes/download.php?id=N`. REST open (469) | `https://www.exploritas.com.br/wp-content/plugins/download-attachments/includes/download.php?id=15099` |
| 38 | Safari | `https://safaricapital.com.br/category/cartas-de-gestao/` | HTML posts | irregular | WP category archive | `https://safaricapital.com.br/carta-macro-novembro-2025/` |
| 39 | Skopos | `https://www.skopos.com.br/blog/cartas-historicas/` | HTML posts | monthly macro letter | Plain. REST 403 | `https://www.skopos.com.br/blog/visao-skopos-carta-macro-setembro/` |
| 40 | Miles | `https://www.milescapital.com.br/cartas/` | PDF via redirect | irregular | `/cartas/<slug>/` redirects to the PDF | `https://www.milescapital.com.br/wp-content/uploads/2024/12/Carta-Miles-Sanepar.pdf` |
| 41 | Ártica | `https://artica.capital/asset-cartas/` | HTML posts plus PDF | about monthly | Plain. REST open (183) | `https://artica.capital/wp-content/uploads/2026/10/Carta-Artica-10.2026-Historia-da-Divida-Brasileira.pdf` |
| 42 | GTI | `https://www.gtinvest.com.br/conteudo/categories/carta-mensal-do-gestor` | HTML (Wix blog) | monthly | Wix; RSS at `/blog-feed.xml` | `https://www.gtinvest.com.br/post/carta-do-gestor-setembro-de-2026` |
| 43 | Augme | `https://www.augme.com.br/cartas-gestao` | PDF (Wix) | monthly | 100 PDFs in static HTML, opaque names | `https://www.augme.com.br/_files/ugd/0405c7_06b8fc152be540f2a9345002f9433212.pdf` |
| 44 | Santander AM | `https://www.santanderassetmanagement.com.br/conteudos/carta-mensal` | PDF | monthly | Plain, 13 PDFs | `https://www.santanderassetmanagement.com.br/content/view/18864/file/Carta%20Mensal%20Julho.pdf` |
| 45 | Nest | `https://nestam.com.br/relatorios/` | PDF | monthly reports | Plain, 388 PDFs. REST open (892) | `https://nestam.com.br/wp-content/uploads/2021/09/2017-08-31-Nest-FIA.pdf` |
| 46 | Fama re.capital | `https://famarecapital.com/conteudos/` | HTML pages | quarterly | Plain links `/project/<slug>/` | `https://famarecapital.com/project/latam-climate-turnaround-relatorio-gestao-1tri2026/` |
| 47 | Galapagos | `https://galapagoscapital.com/conteudo` | PDF | quarterly | Plain, 7 PDFs | `https://galapagoscapital.com/wp-content/uploads/2024/12/AF-Carta-Trimestral-WM-3T24.pdf` |

### B. Opened, letters exist, but needs a specific adapter or was only partly verified (17)

| Manager | What I found | Evidence |
|---|---|---|
| Guepardo | `https://www.guepardoinvest.com.br/cartas-da-gestora/`: quarterly PDFs at site root, back to 2005, year tabs, no pagination. Example `https://www.guepardoinvest.com.br/Carta-118-Carta-aos-Investidores-2T26.pdf`. The host returned 403 to curl (SiteGround CDN); WebFetch got through. | WebFetch only |
| Oceana | `https://www.oceanainvestimentos.com.br/carta-de-gestao/`: semi-annual PDFs back to 2013 under `/mmweb/wp-content/uploads/YYYY/MM/`, e.g. `.../2026/07/Carta-de-Gestao-Oceana-Investimentos-2026S1.pdf`. Same 403 to curl. | WebFetch only |
| Trígono | `https://trigonocapital.com/resenhas/`: HTML posts, paginated, monthly ("Desempenho dos Fundos", "Resenha Mensal", "Carta aos clientes"). Same 403 to curl. | WebFetch only |
| Encore | `https://www.encore.am/midias/`: one very large page (2.3 MB); items like `/midias/comentario-mensal-maio-2026/`; files on `encoresite.blob.core.windows.net`. Whether the body is HTML or PDF: could not verify. | opened |
| Dahlia | `https://www.dahliacapital.com.br/nossas-cartas` (Wix): only 1 PDF in static HTML; the rest is JS. | opened |
| Persevera | `https://www.persevera.com.br/carta-mensal` (Wix): same situation. Example `https://www.persevera.com.br/_files/ugd/056676_c77bf06ade654fd3bc4aa8a0d43a208b.pdf`. | opened |
| Avantgarde | `https://www.avantgardeam.com.br/cartas-da-gestao/`: JS builds URLs from the template `https://avantgarde-site-files.s3.us-east-1.amazonaws.com/carta-gestao/carta-{Y}-{M}.pdf`. I did not fetch a concrete one. | opened (template only) |
| RPS | `https://www.rpscapital.com.br/cartas-gestao`: ASP.NET, downloads via `Download.aspx?Arquivo=<opaque token>`. | opened |
| Forpus | `https://www.forpuscapital.com.br/relatorios-mensais`: same `Download.aspx?Arquivo=` pattern as RPS; 0 PDFs in static HTML. | opened |
| Vinci Compass | `https://www.vincicompass.com/noticias/indice/3` ("Relatórios"): 138 PDFs on `vinciinstitucionalprd.blob.core.windows.net/doc/<numeric id>.pdf`; titles must come from the listing. | opened |
| Indie | No listing page found. Homepage links the current monthly comment: `https://indiecapital.com.br/wp-content/uploads/2026/09/Indie-Capital-Comentario-mensal-Agosto_26.pdf`. REST open (345), so use that. | opened |
| Moat | No listing found. REST open (786) shows monthly `https://www.moat.com.br/cms/wp-content/uploads/2026/10/202609_LB.pdf`. | opened |
| Távola | No listing found. REST open (607) shows monthly reports. | opened |
| Quantitas, Canvas | `/cartas/` and `/conteudo/comunicacoes/` exist, but content loads from MZiQ (`api.mziq.com/mzfilemanager`); 0 PDFs in static HTML; REST 403. | opened |
| Absolute | No letters page on the site. `/fundos/` has 55 PDFs, mostly "Material de Divulgação" under `/wp-content/uploads/oficial-docs/`. Letters appear only on Empiricus and Gorila (snippets). | opened |

### C. Could not verify (do not design against these yet)

| Manager | Status |
|---|---|
| SPX | Site opened (WordPress, REST 401). No public letter listing found; footer says "Assine nossa carta mensal" (email). Indexed PDFs under `spxcapital.com/wp-content/uploads/` look like fund fact sheets (snippet). Page carries a geographic-eligibility notice. |
| Ibiuna | Site opened. No listing found; "Comentários mensais" offered via a newsletter form (`forms.office.com`). REST open (193) but the recent items are regulatory. Copies exist on Acionista and Gorila. |
| Gávea | 403 Cloudflare challenge to both clients. Snippets show `https://anexos.gaveainvest.com.br/site/carta-fundos-macro-04-2024.pdf`. |
| XP Asset | 403 (Akamai). Snippets show letters on `api.mziq.com/mzfilemanager/v2/d/e555cf0b-57b4-4ed0-bdc1-663adbde42f1/<id>`. |
| Itaú Asset | Homepage 200, `/insights/` 403 (Akamai). Snippets show PDFs at `assetfront.arquivosparceiros.cloud.itau.com.br/FND/...`. |
| BTG Asset | JS shell only. |
| Vinland | JS single-page app. Even `https://vinlandcap.com/pdf/carta/Carta_Mensal_2026_08.pdf` returned the 2,545-byte HTML shell. Snippets show the pattern `/pdf/carta/Carta_Mensal_YYYY_MM.pdf` for 2020 to 2022. |
| Velt | Nuxt front end over DatoCMS GraphQL; no letter page found in static HTML. |
| Constellation | Site opened. `/documentos-relevantes/` (38 regulatory PDFs) and `/leituras/` (no PDFs). No letters found. REST 401. |
| Real Investor, Núcleo, Alpha Key, Lakewood, Tarpon, Sharp | Sites opened; no letter listing found. |
| Tork, Clave, Truxt, Garde | Connection failures from my environment: DNS not resolving (Tork), expired certificate (Clave), incomplete certificate chain (Truxt), TLS handshake failure (Garde). Not evidence the sites are down. Snippet gives `https://www.garde.com.br/cartas-ao-gestor`. |
| Pandhora, Apex Capital, Mos, Gap, Icatu Vanguarda | Letter or report pages exist (`pandhora.com/cartas-mensais`, `apexcapital.com.br/cartas`, `moscapital.com.br/pages/cartas.html`, `gapasset.com.br/relatorios`, `icatuvanguarda.com.br/relatorios`) but show 0 PDFs in static HTML (JS-rendered). |
| WHG, Giant Steps | 403 Cloudflare challenge and 429 respectively. |

## 4. Patterns for a generic collector

1. **WordPress media REST (highest yield).** `GET /wp-json/wp/v2/media?mime_type=application/pdf&per_page=100&page=N&_fields=date,source_url,title` gives every uploaded PDF with upload date, title, and total in the `X-WP-Total` header.
   - Open on about 40 of 50 WordPress sites tested, including Kapitalo, Dynamo, Squadra, JGP, Legacy, Vista, Ace, Adam, Bahia, Bogari, STK, Navi, Leblon, Moat, Indie, IP, Távola, Nest, Vokin, Meta, Ártica.
   - Closed: Brasil Capital, SPX, Constellation, Alaska (401); the MZiQ-themed sites (403); Absolute (404).
   - It returns all PDFs, including regulations, fact sheets and policies. You need a filename/title classifier (`carta|comentario|relatorio.*(gestao|mensal)|letter` versus `regulamento|lamina|politica|DOC_REGUL|formulario`).
   - Upload date is not the reference period. Several sites overwrite into old folders (Kapitalo and Leblon upload 2026 files under `/2020/02/`), so parse the period from the filename or the PDF text.
2. **Plain listing page with `.pdf` anchors.** Covers most of table A, including the non-WordPress ones (Atmos, Genoa, AZ Quest, Santander, Sparta, 3R). One collector: fetch, extract anchors, use the anchor text as title.
3. **HTML-native letters.** Kinea, Neo, Trígono, Safari, Skopos, GTI, Kadima, Leblon letters, Dynamo and Ártica posts. Archive the HTML; for WordPress, the posts REST endpoint by category is cleaner.
4. **Common slugs worth probing on any manager domain.** `/cartas/`, `/cartas-mensais/`, `/carta-do-gestor/`, `/cartas-de-gestao/`, `/cartas-da-gestora/`, `/relatorios/`, `/relatorios-mensais/`, `/conteudo/`, `/conteudos/`, `/biblioteca/`, `/publicacoes/`, `/documentos/`. My homepage link scan (keywords `cart|relat|coment|conteud|publica`) found the right page on most sites.
5. **Predictable filenames, good for incremental polling.** Opportunity `CartaGestao_YYYYMM.pdf`; Sparta `CartaMensal_YYYYMM.pdf`; Genoa `CartaMensalGenoaCapital_MmmYY.pdf`; Verde `Verde-REL-YYYY_MM.pdf`; Avantgarde `carta-{Y}-{M}.pdf`.
6. **Shared vendors.**
   - Wix (Augme, Dahlia, Persevera, GTI, Arbor, Root): `/_files/ugd/<hash>.pdf`, blog RSS at `/blog-feed.xml`.
   - MZiQ (Quantitas, Canvas, Kadima, Skopos, BLP, SulAmérica, Porto, BB, Bradesco, and XP Asset per snippet): files on `api.mziq.com/mzfilemanager/v2/d/<tenant>/<file>` or `filemanager-cdn.mziq.com/published/<tenant>/...`. One adapter could cover all of them; I did not reverse the listing call.
   - The `Download.aspx?Arquivo=<token>` ASP.NET vendor (RPS, Forpus).
7. **Cases needing a specific adapter or a headless browser.** Verde (JSON, easy); Legacy (admin-ajax); Exploritas (download.php ids); Vinland, BTG, Velt, Apex, Pandhora, Icatu, Gap (JS apps); Vinci and Encore (Azure blob ids); and everything behind Cloudflare, Akamai or SiteGround bot filtering (Gávea, WHG, XP Asset, Itaú, Oceana, Guepardo, Trígono).
8. **Dedup.** The same letter appears on the manager site, Empiricus, Gorila, Acionista and distributor portals (Paraná Banco, Ágora). Hash the PDF bytes and keep the manager's site as canonical.

## 5. Legal and etiquette

- **Reproduction clauses are common but not universal.** Of the 5 sample PDFs I parsed:
  - Verde: "Este material não pode ser copiado, reproduzido ou distribuído sem a prévia e expressa concordância da Verde."
  - Kapitalo: "Este conteúdo não pode ser copiado, reproduzido ou distribuído sem a prévia e expressa concordância das gestoras."
  - No such clause found in the Dynamo 128, Genoa Oct 2025 or Squadra 2011 letters.
- **What that means for you.** Letters are copyrighted works regardless of any notice. The clauses target copying and redistribution; a private archive is the lowest-risk use, but a RAG that shows verbatim passages to anyone else would count as distribution. I am not giving a legal opinion on Lei 9.610.
- **No manager site I sampled forbids crawling in robots.txt.** Where one exists it is either open or the WordPress default (`Disallow: /wp-admin/`). Specific rules worth honouring:
  - Bogari: `Crawl-delay: 1`.
  - Kinea: disallows `/category/`, `/pdf/`, `/author/`.
  - M Square: `Disallow: /wp-json/`.
  - Empiricus: `Disallow: /wp-json/`.
  - CVM: `Disallow: /api/`, `Crawl-Delay: 10`.
- **Explicit anti-automation signals.**
  - Bom Dia Mercado's robots blocks AI agents by name.
  - Rio Bravo and Bradesco Asset robots have GPTBot / Google-Extended sections; I did not read what they say.
  - Empiricus's terms prohibit copying, compiling and circumventing its technology.
  - Acionista, Gávea and WHG serve Cloudflare challenges; XP Asset and Itaú return Akamai 403; the SiteGround-hosted sites (Oceana, Guepardo, Trígono) returned 403 to a plain client. Treat a challenge as a "no" for unattended collection; working around it is a different decision from polite crawling.
- **Terms pages:** I read only Empiricus's. Manager-site terms of use: could not verify.
- **SPX** shows a geographic-eligibility notice (Brazil only; not EEA or US residents) on its funds section.
- **Etiquette that fits what I saw.** Identify the client in the user agent, one request every few seconds per host, use `If-Modified-Since`/ETag on listing pages, and poll monthly; most letters land in the first 10 days of the month.
