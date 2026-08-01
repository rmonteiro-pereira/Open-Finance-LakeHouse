# Guia de Acesso às Ferramentas do Cluster Kubernetes

Este documento descreve as ferramentas da plataforma e **como descobrir** o endereço e as
credenciais de cada uma. Ele é um runbook genérico: **nenhum hostname externo, nenhum IP e
nenhuma credencial real aparecem aqui**. Os únicos endereços citados são nomes DNS internos do
cluster (`servico.namespace.svc.cluster.local`), que só resolvem de dentro dele e são os nomes
padrão dos charts.

> **Sem credenciais neste arquivo — por design.**
> Todos os segredos da plataforma vivem em `Secret`/`SealedSecret` do Kubernetes. Este guia
> mostra o *comando* que lê cada credencial, nunca o valor. Se um serviço ainda estiver com
> credencial padrão de fábrica (por exemplo, o `admin` inicial do Grafana), **troque antes de
> expor o serviço** — a seção [Checklist de hardening](#-checklist-de-hardening) cobre isso.

## 🔤 Convenções deste guia

Substitua os marcadores abaixo pelos valores do seu ambiente:

| Marcador | Significado | Como obter |
|---|---|---|
| `<BASE_DOMAIN>` | Domínio base que serve os Ingresses | `kubectl get ingress -A -o wide` |
| `<INGRESS_IP>` | IP do Ingress Controller (rede interna) | `kubectl get svc -n ingress-nginx -o wide` |
| `<NODE_IP>` | IP de um node do cluster (rede interna) | `kubectl get nodes -o wide` |

---

## 🌐 Informações Gerais

- **Cluster Kubernetes**: Talos Linux
- **Domínio Base**: `<BASE_DOMAIN>`
- **Ingress Controller**: nginx
- **Protocolo**: HTTP por padrão — ver [Checklist de hardening](#-checklist-de-hardening)

Para listar o que está realmente exposto hoje (fonte da verdade, em vez desta tabela):

```bash
kubectl get ingress -A -o wide
```

---

## 📊 Plataforma de Dados

### 1. **Grafana** — Observabilidade e Dashboards

**Acesso Web UI:**
- **URL**: `http://grafana.<BASE_DOMAIN>`
- **Credenciais**: lidas do Secret do release (namespace `monitoring`):
  ```bash
  kubectl get secret grafana -n monitoring -o jsonpath='{.data.admin-user}'     | base64 -d
  kubectl get secret grafana -n monitoring -o jsonpath='{.data.admin-password}' | base64 -d
  ```
  ⚠️ O chart do Grafana sobe com um usuário `admin` de fábrica. **Troque a senha e mova-a
  para um `SealedSecret` antes de expor a UI** — senha padrão em serviço alcançável é
  exposição, não conveniência.

**Funcionalidades:**
- Visualização de métricas do cluster (CPU, memória, rede, etc.)
- Dashboards pré-configurados:
  - Kubernetes Cluster Monitoring (ID: 7249)
  - Node Exporter (ID: 1860)
  - Loki Logs (ID: 13639)
- Integração com Prometheus (métricas) e Loki (logs)
- Query builder para criar dashboards customizados

**Datasources Configurados:**
- Prometheus: `http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090`
- Loki: `http://loki-gateway.loki.svc.cluster.local`

---

### 2. **Airflow** — Orquestração de Workflows

**Acesso Web UI:**
- **URL**: `http://airflow.<BASE_DOMAIN>`
- **Credenciais**: Secret `airflow-admin-credentials` (namespace `airflow`):
  ```bash
  kubectl get secret airflow-admin-credentials -n airflow -o jsonpath='{.data.username}' | base64 -d
  kubectl get secret airflow-admin-credentials -n airflow -o jsonpath='{.data.password}' | base64 -d
  ```

**Funcionalidades:**
- Gerenciamento de DAGs (Directed Acyclic Graphs)
- Monitoramento de execuções de workflows
- Visualização de logs de tasks
- Integração com Spark para processamento de dados
- Execução agendada de jobs

**Backend:**
- Database: PostgreSQL (`postgresql.postgres.svc.cluster.local:5432`)
- Queue: Redis (`redis-master-master.redis.svc.cluster.local:6379`)

---

### 3. **Apache Spark** — Processamento de Dados

**Acesso Web UI (Spark Master):**
- **URL**: `http://spark.<BASE_DOMAIN>`
- **Sem autenticação** — mantenha restrito à rede interna (ver hardening)

**Funcionalidades:**
- Spark Master UI (porta 8080)
- Monitoramento de aplicações Spark em execução
- Visualização de executors e workers
- Logs de aplicações
- Suporte para Delta Lake e S3 (MinIO)

**Configuração Delta Lake / S3:**
- S3 Endpoint: `http://minio.minio.svc.cluster.local:9000`
- Access Key / Secret Key: **injetados via Secret `minio-secrets`** (namespace `minio`) nas
  variáveis `MINIO_USER` / `MINIO_PASSWORD` do pod — nunca escritos em código ou em docs.
  Ver [Como ler as credenciais do MinIO](#6-minio--armazenamento-s3-compatible).
- Formato de dados: Delta Lake (versionamento de dados)

**Arquitetura:**
- 1 Master (UI na porta 8080)
- 1 Worker (métricas na porta 8081)

---

### 4. **OpenMetadata** — Catálogo de Metadados

**Acesso Web UI:**
- **URL**: `http://openmetadata.<BASE_DOMAIN>` (confirme com `kubectl get ingress -n openmetadata`)
- **Credenciais**: Secret do release, no namespace `openmetadata`

**Funcionalidades:**
- Catálogo de dados e metadados
- Discovery de schemas de tabelas
- Linhagem de dados
- Integração com múltiplas fontes de dados

**Backend:**
- Database: PostgreSQL (`postgresql.postgres.svc.cluster.local:5432`)
- Elasticsearch: Para busca e indexação
- Barramento de eventos: para streaming de metadados

**Para acessar temporariamente (sem Ingress):**
```bash
kubectl port-forward -n openmetadata svc/openmetadata 8080:8080
# Acesse: http://localhost:8080
```

---

### 5. **LakeFS** — Versionamento de Dados

**Acesso Web UI:**
- **URL**: `http://lakefs.<BASE_DOMAIN>`
- **Alternativa (NodePort)**: `http://<NODE_IP>:30081`
- **Credenciais**: criadas no primeiro acesso (setup wizard) ou via CLI; guarde-as em um
  gerenciador de segredos, não neste repositório

**Funcionalidades:**
- Versionamento de dados no estilo Git
- Branches e commits para dados
- Merge de branches de dados
- Integração com S3 (MinIO) como backend
- Gerenciamento de repositórios e branches

**Backend:**
- S3 Storage: MinIO (`http://minio.minio.svc.cluster.local:9000`)
- Database: PostgreSQL (`postgresql.postgres.svc.cluster.local:5432`)
- Credenciais S3: Secret `minio-secrets` (namespace `minio`)

**Configuração de Repositório:**
```yaml
S3 Endpoint: http://minio.minio.svc.cluster.local:9000
Access Key: ${MINIO_USER}        # do Secret minio-secrets
Secret Key: ${MINIO_PASSWORD}    # do Secret minio-secrets
Path Style: true
```

---

## 💾 Armazenamento

### 6. **MinIO** — Armazenamento S3-Compatible

**Acesso Web UI (Console):**
- **URL**: `http://minio-ui.<BASE_DOMAIN>/console/`
- **Credenciais**: Secret `minio-secrets` (namespace `minio`):
  ```bash
  kubectl get secret minio-secrets -n minio -o jsonpath='{.data.root-user}'     | base64 -d
  kubectl get secret minio-secrets -n minio -o jsonpath='{.data.root-password}' | base64 -d
  ```

**API S3:**
- **Endpoint API**: `http://minio-api.<BASE_DOMAIN>`
- **Porta**: 9000 (API S3)
- **Porta**: 9001 (Console UI)

**Funcionalidades:**
- Interface web para gerenciamento de buckets
- API S3 compatível (usar com boto3, s3cmd, etc.)
- Suporte para políticas de acesso
- Replicação e distribuição de dados (4 nós)

**Exemplo de Uso (Python)** — credenciais sempre vindas do ambiente, nunca literais:

```python
import os

from minio import Minio

client = Minio(
    os.environ["MINIO_ENDPOINT"],          # ex.: minio-api.<BASE_DOMAIN>
    access_key=os.environ["MINIO_USER"],
    secret_key=os.environ["MINIO_PASSWORD"],
    # HTTP por padrão porque nenhum Ingress declara `spec.tls` hoje; assim que
    # cert-manager estiver no lugar, defina MINIO_SECURE=true.
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
)
```

Estas são as mesmas variáveis que o pacote `ofl` já lê via `pydantic-settings`
(ver `ofl/config.py`), então o pipeline e este runbook usam exatamente a mesma fonte.

**Configuração:**
- 4 nós (minio-0, minio-1, minio-2, minio-3)
- Storage distribuído
- Erasure coding para redundância

---

## 🔧 Serviços de Infraestrutura

### 7. **PostgreSQL** — Banco de Dados

**Acesso via kubectl:**
```bash
kubectl port-forward -n postgres svc/postgresql-postgresql 5432:5432
# Conectar com cliente PostgreSQL:
# psql -h localhost -p 5432 -U <usuario> -d <database>
```

**Credenciais:**
- Secret no namespace `postgres` (nome do Secret varia com o release):
  ```bash
  kubectl get secrets -n postgres
  kubectl get secret <postgres-secret> -n postgres -o jsonpath='{.data.postgres-password}' | base64 -d
  ```

**Databases Criados:**
- `airflow` — Para Airflow
- `openmetadata_db` — Para OpenMetadata
- `lakefs_db` — Para LakeFS

**Para listar databases:**
```bash
kubectl exec -it -n postgres <postgres-pod-name> -- psql -U postgres -c "\l"
```

---

### 8. **Redis** — Cache e Queue

**Acesso via kubectl:**
```bash
kubectl port-forward -n redis svc/redis-master-master 6379:6379
# Conectar com redis-cli:
# redis-cli -h localhost -p 6379
```

**Uso:**
- Queue backend para Airflow
- Cache para aplicações
- Pub/Sub para eventos

---

### 9. **Elasticsearch** — Busca e Indexação

**Acesso via kubectl:**
```bash
kubectl port-forward -n elasticsearch svc/elasticsearch-master 9200:9200
# Testar: curl http://localhost:9200
```

**Credenciais:**
- Secret `elasticsearch-master-credentials` (namespace `elasticsearch`):
  ```bash
  kubectl get secret elasticsearch-master-credentials -n elasticsearch \
    -o jsonpath='{.data.password}' | base64 -d
  ```

**Funcionalidades:**
- Busca full-text
- Indexação de documentos
- Analytics e agregações
- Usado pelo OpenMetadata para busca de metadados

---

### 10. **Barramento de Eventos (Kafka-compatible)** — Streaming

**Acesso via kubectl:**
```bash
kubectl get pods -n kafka
kubectl port-forward -n kafka <pod> 9092:9092
```

**Configuração:**
- Modo KRaft (sem ZooKeeper)
- Tópicos gerenciados via CLI/ferramentas Kafka
- Usado pelo OpenMetadata para eventos de metadados e pela lane de lineage

**Bootstrap Servers (interno, DNS do cluster):**
```
<broker>-0.kafka-headless.kafka.svc.cluster.local:9092
<broker>-1.kafka-headless.kafka.svc.cluster.local:9092
<broker>-2.kafka-headless.kafka.svc.cluster.local:9092
```

---

## 📈 Observabilidade

### 11. **Prometheus** — Coleta de Métricas

**Acesso via kubectl:**
```bash
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090
# Acesse: http://localhost:9090
```

**Funcionalidades:**
- Coleta de métricas do cluster Kubernetes
- Métricas de nodes (via node-exporter)
- Métricas de aplicações (Spark, Airflow, etc.)
- Alertas e regras de recording
- Service discovery automático

**Targets Monitorados:**
- Kubernetes API server
- kubelet (cAdvisor)
- node-exporter (métricas de hardware)
- Aplicações com annotations `prometheus.io/scrape: "true"`

---

### 12. **Loki** — Agregação de Logs

**Acesso Web UI:**
- Integrado ao Grafana (Datasource: Loki)
- Não possui UI própria

**Acesso via kubectl (API):**
```bash
kubectl port-forward -n loki svc/loki-gateway 3100:80
# API: http://localhost:3100
```

**Funcionalidades:**
- Agregação de logs de todos os pods
- Query via LogQL (similar a PromQL)
- Integração com Grafana para visualização
- Logs coletados via Promtail (DaemonSet)

**Query no Grafana:**
```
{namespace="airflow"} |= "ERROR"
{container="spark-worker"} | json
```

---

### 13. **Promtail** — Coleta de Logs

**Status:**
- DaemonSet rodando em todos os nodes
- Coleta logs automaticamente de `/var/log/pods` e `/var/log/containers`
- Envia logs para Loki

**Verificar status:**
```bash
kubectl get daemonset -n loki
kubectl get pods -n loki -l app.kubernetes.io/name=promtail
```

---

## 🔐 Gerenciamento de Segredos

### 14. **Sealed Secrets**

Esta é a razão pela qual **nenhuma credencial aparece neste arquivo**: os segredos são
encriptados com a chave pública do controller, versionados como `SealedSecret`, e só o
controller dentro do cluster consegue desencriptá-los.

**Verificar:**
```bash
# Listar SealedSecrets (seguros para versionar)
kubectl get sealedsecrets -A

# Ler o Secret já desencriptado pelo controller (NÃO cole a saída em docs/PRs)
kubectl get secret <secret-name> -n <namespace> -o yaml
```

**Selar um novo segredo** — sem passar o valor na linha de comando (ela vai parar no
histórico do shell e na lista de processos):

```bash
umask 077
tmp="$(mktemp)"
printf '%s' 'VALOR_DO_SEGREDO' > "$tmp"      # ou: read -rs -p 'valor: ' v; printf '%s' "$v" > "$tmp"

kubectl create secret generic <name> -n <ns> \
  --from-file=<key>="$tmp" --dry-run=client -o yaml \
| kubeseal --format yaml > <name>-sealed.yaml   # este arquivo pode ir para o Git

shred -u "$tmp" 2>/dev/null || rm -f "$tmp"
```

---

## 📝 Flux CD — GitOps

**Status do Flux:**
```bash
kubectl get gitrepositories -n flux-system
kubectl get kustomizations -n flux-system
kubectl get helmreleases -A
```

**Funcionalidades:**
- Sincronização automática do repositório Git
- Deploy de aplicações via Helm e Kustomize
- Reconciliação contínua do estado desejado

---

## 🔍 Comandos Úteis para Diagnóstico

### Listar todos os serviços expostos:
```bash
kubectl get svc -A
kubectl get ingress -A
```

### Verificar pods em execução:
```bash
kubectl get pods -A
```

### Acessar logs de um pod:
```bash
kubectl logs -n <namespace> <pod-name> -f
```

### Descrever recursos para debug:
```bash
kubectl describe pod -n <namespace> <pod-name>
kubectl describe svc -n <namespace> <service-name>
kubectl describe ingress -n <namespace> <ingress-name>
```

### Executar comandos dentro de um pod:
```bash
kubectl exec -it -n <namespace> <pod-name> -- /bin/bash
```

---

## 📋 Resumo de URLs

| Serviço | URL | Onde estão as credenciais |
|---------|-----|---------------------------|
| Grafana | `http://grafana.<BASE_DOMAIN>` | Secret `grafana` (ns `monitoring`) |
| Airflow | `http://airflow.<BASE_DOMAIN>` | Secret `airflow-admin-credentials` (ns `airflow`) |
| Spark | `http://spark.<BASE_DOMAIN>` | Sem auth — restringir à rede interna |
| LakeFS | `http://lakefs.<BASE_DOMAIN>` | Criadas no setup; NodePort `<NODE_IP>:30081` |
| MinIO UI | `http://minio-ui.<BASE_DOMAIN>/console/` | Secret `minio-secrets` (ns `minio`) |
| MinIO API | `http://minio-api.<BASE_DOMAIN>` | Secret `minio-secrets` (ns `minio`) |
| OpenMetadata | `http://openmetadata.<BASE_DOMAIN>` | Secret do release (ns `openmetadata`) |
| Prometheus | Port-forward: 9090 | Sem auth |
| PostgreSQL | Port-forward: 5432 | Secret no ns `postgres` |
| Redis | Port-forward: 6379 | Secret no ns `redis` (se auth habilitada) |
| Elasticsearch | Port-forward: 9200 | Secret `elasticsearch-master-credentials` |

---

## ⚠️ Checklist de hardening

Este é um cluster de laboratório de **um node**, e vários charts sobem com padrões
convenientes em vez de seguros. Antes de expor qualquer coisa fora da rede interna:

1. **Trocar toda credencial de fábrica.** Charts como o do Grafana e o do MinIO sobem com
   usuário/senha padrão documentados publicamente. Credencial padrão + serviço alcançável =
   serviço comprometido. Troque, sele como `SealedSecret`, e nunca escreva o valor em um
   arquivo versionado.
2. **TLS no Ingress.** Emitir certificados via cert-manager e declarar `spec.tls`; hoje os
   Ingresses respondem em HTTP puro, o que envia credenciais em texto claro.
3. **Autenticação nas UIs sem auth.** A Spark Master UI e o Prometheus não têm login —
   coloque-os atrás de um proxy autenticado ou mantenha-os apenas na rede interna/VPN.
4. **NetworkPolicies** para limitar tráfego pod-a-pod entre namespaces.
5. **Resource Quotas / Limit Ranges** por namespace.
6. **Backups** dos volumes críticos (PostgreSQL, MinIO) e procedimento de restore testado.
7. **Nunca colar valores de segredo** em issues, PRs, documentação ou logs — os comandos
   deste guia leem os segredos localmente; a saída deles não deve sair do seu terminal.

## 🚀 Próximos Passos Sugeridos

1. Configurar TLS/HTTPS com cert-manager
2. Implementar autenticação SSO (OAuth/OIDC)
3. Configurar backup automatizado dos dados críticos
4. Implementar políticas de rede (NetworkPolicies)
5. Configurar Resource Quotas e Limit Ranges
6. Documentar procedures de disaster recovery

---

**Escopo**: runbook genérico da plataforma. Hostnames, IPs e credenciais reais vivem no
repositório de infraestrutura (privado) e nos Secrets do cluster — deliberadamente **não** aqui.
