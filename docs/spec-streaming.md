# Streaming em Spark a ~R$0 (lane do Open-Fin
ance-LakeHouse / P2)
**Objetivo:** demonstrar
 **Spark Structured Streaming** de verdade (e
vent-time, watermark, dedup, estado, exactly-
once) **sem gastar** — como uma **lane de s
treaming adicionada ao lakehouse** (não um 5
º projeto). Dá ao P2 a narrativa **batch + 
streaming (Lambda/Kappa)**.

> Frase-alvo de 
entrevista: *"Meu lakehouse tem lane batch e 
lane streaming. O streaming é Spark Structur
ed Streaming com janelas de event-time, water
mark e exactly-once por checkpoint — e roda
 a R$0 porque uso Trigger.AvailableNow num cr
on em vez de cluster ligado."*

---

## Por q
ue streaming "custa" e como zerar (SEM depend
er do homelab)
O custo é **cluster ligado 24
/7**, não o Spark. E o **homelab não fica s
empre ligado** — então a camada "live" **n
ão roda nele**. Separação:
- **Compute do 
live:** **GitHub Actions runner** (efêmero, 
grátis em repo público) — roda o Spark em
 local mode. Opcional p/ 24/7 real: **Oracle 
Cloud Always Free** (VM ARM grátis pra sempr
e, 2 OCPU/12GB).
- **Storage durável:** **Cl
oudflare R2** (10GB grátis, egress zero, S3-
compatível) guarda Delta + checkpoint entre 
runs efêmeros. (substitui o MinIO do homelab
 p/ o live)
- **Homelab:** só **dev pesado /
 runs reproduzíveis / gravar demo** quando e
stiver ligado.

### Modo 1 — Dev local (no 
homelab/laptop, quando ligado)
Docker local p
ra sentir a semântica e gravar GIF/vídeo. *
*Não** é a camada live.

### Modo 2 — "Li
ve" a R$0, na nuvem (recomendado) — `Trigge
r.AvailableNow` + cron + R2
Job que lê o **d
elta desde o último checkpoint** (no R2), pr
ocessa incremental **mantendo semântica de s
treaming** (checkpoints, watermark, exactly-o
nce) e **encerra**. Roda no **GitHub Actions 
cron** (não no homelab), estado no **R2**. "
Compute starts in seconds, runs the batch, sh
uts down" — padrão mais custo-eficiente se
gundo a Databricks.

## Fonte viva grátis (s
em auth)
- **Cripto WebSocket** (Binance/Coin
base/Kraken) — ticks de mercado, **combina 
com o tema financeiro do lakehouse**. (proxy 
grátis de "market data")
- **Wikimedia Event
Streams** (SSE) — edits em tempo real, zero
 config.
- Listas: conduktor/public-streaming
-api, bytewax/awesome-public-real-time-datase
ts.

## Stack (100% aberta)
| Camada | Escolh
a | Nota |
|---|---|---|
| Fonte | WS cripto 
ou Wikimedia SSE | grátis, sem key |
| Broke
r (opcional) | Upstash Kafka free (10K/dia) /
 Redpanda Serverless free (ou Kafka local só
 no dev) | ou só arquivos append no R2 |
| C
ompute (live) | **GitHub Actions runner** (ef
êmero) — Spark local mode. 24/7 opcional: 
**Oracle Cloud Always Free** | não é o home
lab |
| Processamento | **Spark Structured St
reaming** (PySpark) | aberto |
| Storage dur�
�vel | **Cloudflare R2** (10GB grátis, egres
s 0, S3-compat) | Delta + checkpoint sobreviv
em entre runs |
| Sink | **Delta** (bronze st
reaming, no R2) → silver/gold do lakehouse 
consome | reusa o P2 |
| Scheduler | **GitHub
 Actions cron** (Trigger.AvailableNow) | grá
tis repo público |

## Conceitos a demonstra
r (o que é sênior)
- **Event-time vs proces
sing-time** + **watermark** (tratar late data
 honestamente).
- **Deduplicação** por chav
e (`dropDuplicatesWithinWatermark`).
- **Agre
gação com estado** (janelas tumbling/slidin
g: ex. OHLC por minuto).
- **Exactly-once** v
ia checkpoint + sink idempotente (Delta MERGE
).
- **Schema evolution** e dead-letter para 
eventos malformados.
- **Backpressure** e `ma
xOffsetsPerTrigger` / `maxFilesPerTrigger`.


## Milestones
**M0 —** Producer: lê o WS/S
SE e grava eventos append (JSON) em MinIO/loc
al (ou publica no Kafka local).
**M1 —** Sp
ark Structured Streaming lê a fonte → escr
eve **bronze Delta** (append, com checkpoint)
. Rodar contínuo local e ver micro-batches.

**M2 —** Janela de event-time + watermark �
�� agregação com estado (ex.: OHLC/volume p
or 1min) → **silver Delta**.
**M3 —** Tro
car o trigger para **`Trigger.AvailableNow`**
: mesmo código, processa o delta e encerra. 
Provar idempotência (rodar 2× = mesmo resul
tado).
**M4 —** `streaming.yml` (GitHub Act
ions cron, ex.: a cada 15min): sobe Spark, ro
da AvailableNow, atualiza Delta no R2, commit
a um snapshot de métricas JSON (lag, through
put, contagem) → **dashboard React** (Verce
l/CF Pages, não dorme) lê os artefatos.
**M
5 —** Integrar ao lakehouse: o **silver/gol
d existente** consome a tabela streaming → 
um mart "near-real-time" no DuckDB. Writeup c
om diagrama batch+streaming.

## Onde encaixa

- **Recomendado:** lane nova no repo **Open-
Finance-LakeHouse** (P2) — fortalece o flag
ship com "batch + streaming".
- Alternativa: 
repo standalone `spark-streaming-showcase` se
 preferir peça isolada.

## Kickoff
1. Escol
he a fonte (WS cripto pra casar com finanças
).
2. Producer grava eventos append em `bronz
e/_landing/`.
3. Job Spark Structured Streami
ng (`readStream` → `writeStream` Delta) rod
ando **local contínuo** — vê os micro-bat
ches.
4. Depois troca pra `Trigger.AvailableN
ow` + `streaming.yml` no cron → vira "live"
 a R$0.

> Regra: comece **local contínuo** 
pra sentir a semântica; só depois migre pro
 `AvailableNow`+cron pro custo-zero. O checkp
oint é o que garante exactly-once nos dois m
odos.


