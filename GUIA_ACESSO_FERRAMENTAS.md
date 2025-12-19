# Guia de Acesso às Ferramentas do Cluster Kubernetes

Este documento descreve todas as ferramentas disponíveis no cluster Kubernetes e como acessá-las.

## 🌐 Informações Gerais

- **Cluster Kubernetes**: Talos Linux (192.168.32.24)
- **Domínio Base**: `vanir-proxmox.duckdns.org`
- **Ingress Controller**: nginx
- **Protocolo**: HTTP (TLS desabilitado por padrão)

---

## 📊 Plataforma de Dados

### 1. **Grafana** - Observabilidade e Dashboards

**Acesso Web UI:**
- **URL**: `http://grafana.vanir-proxmox.duckdns.org`
- **Credenciais Padrão:**
  - Usuário: `admin`
  - Senha: `admin`
  - ⚠️ **IMPORTANTE**: Altere a senha padrão em produção!

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

### 2. **Airflow** - Orquestração de Workflows

**Acesso Web UI:**
- **URL**: `http://airflow.vanir-proxmox.duckdns.org`
- **Credenciais:**
  - Usuário: Definido no Secret `airflow-admin-credentials` (namespace: `airflow`)
  - Senha: Definida no Secret `airflow-admin-credentials` (namespace: `airflow`)
  - 💡 Verifique os secrets do Kubernetes para obter as credenciais:
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

### 3. **Apache Spark** - Processamento de Dados

**Acesso Web UI (Spark Master):**
- **URL**: `http://spark.vanir-proxmox.duckdns.org`
- **Sem autenticação** (acesso direto)

**Funcionalidades:**
- Spark Master UI (porta 8080)
- Monitoramento de aplicações Spark em execução
- Visualização de executors e workers
- Logs de aplicações
- Suporte para Delta Lake e S3 (MinIO)

**Configuração Delta Lake:**
- S3 Endpoint: `http://minio.minio.svc.cluster.local:9000`
- Access Key: `minio`
- Secret Key: `minio123`
- Formato de dados: Delta Lake (versionamento de dados)

**Arquitetura:**
- 1 Master (UI na porta 8080)
- 1 Worker (métricas na porta 8081)

---

### 4. **OpenMetadata** - Catálogo de Metadados

**Acesso Web UI:**
- **URL**: Não configurado via Ingress (verificar NodePort ou port-forward)
- **Status**: Verificar no namespace `openmetadata`

**Funcionalidades:**
- Catálogo de dados e metadados
- Discovery de schemas de tabelas
- Linhagem de dados
- Integração com múltiplas fontes de dados

**Backend:**
- Database: PostgreSQL (`postgresql.postgres.svc.cluster.local:5432`)
- Elasticsearch: Para busca e indexação
- Kafka: Para eventos e streaming de metadados

**Para acessar temporariamente:**
```bash
kubectl port-forward -n openmetadata svc/openmetadata 8080:8080
# Acesse: http://localhost:8080
```

---

### 5. **LakeFS** - Versionamento de Dados

**Acesso Web UI:**
- **URL**: `http://lakefs.vanir-proxmox.duckdns.org`
- **Alternativa (NodePort)**: `http://192.168.32.24:30081`
- **Credenciais**: Criar conta no primeiro acesso ou via CLI

**Funcionalidades:**
- Versionamento de dados no estilo Git
- Branches e commits para dados
- Merge de branches de dados
- Integração com S3 (MinIO) como backend
- Gerenciamento de repositórios e branches

**Backend:**
- S3 Storage: MinIO (`http://minio.minio.svc.cluster.local:9000`)
- Database: PostgreSQL (`postgresql.postgres.svc.cluster.local:5432`)
- Credenciais S3: `minio` / `minio123`

**Configuração de Repositório:**
```yaml
S3 Endpoint: http://minio.minio.svc.cluster.local:9000
Access Key: minio
Secret Key: minio123
Path Style: true
```

---

## 💾 Armazenamento

### 6. **MinIO** - Armazenamento S3-Compatible

**Acesso Web UI (Console):**
- **URL**: `https://minio-ui.vanir-proxmox.duckdns.org/console/`
- **Credenciais**: Definidas no Secret `minio-secrets` (namespace: `minio`)
  - Root User: Verificar no secret
  - Root Password: Verificar no secret
  - 💡 Verificar credenciais:
    ```bash
    kubectl get secret minio-secrets -n minio -o jsonpath='{.data.root-user}' | base64 -d
    kubectl get secret minio-secrets -n minio -o jsonpath='{.data.root-password}' | base64 -d
    ```

**API S3:**
- **Endpoint API**: `http://minio-api.vanir-proxmox.duckdns.org`
- **Porta**: 9000 (API S3)
- **Porta**: 9001 (Console UI)

**Funcionalidades:**
- Interface web para gerenciamento de buckets
- API S3 compatível (usar com boto3, s3cmd, etc.)
- Suporte para políticas de acesso
- Replicação e distribuição de dados (4 nós)

**Exemplo de Uso (Python):**
```python
from minio import Minio

client = Minio(
    "minio-api.vanir-proxmox.duckdns.org",
    access_key="ROOT_USER_FROM_SECRET",
    secret_key="ROOT_PASSWORD_FROM_SECRET",
    secure=False  # HTTP, não HTTPS
)
```

**Configuração:**
- 4 nós (minio-0, minio-1, minio-2, minio-3)
- Storage distribuído
- Erasure coding para redundância

---

## 🔧 Serviços de Infraestrutura

### 7. **PostgreSQL** - Banco de Dados

**Acesso via kubectl:**
```bash
kubectl port-forward -n postgres svc/postgresql-postgresql 5432:5432
# Conectar com cliente PostgreSQL:
# psql -h localhost -p 5432 -U <usuario> -d <database>
```

**Credenciais:**
- Verificar secrets no namespace `postgres`
- Usuário padrão geralmente: `postgres`
- Password: Definido no Secret `postgresql-postgresql` ou similar

**Databases Criados:**
- `airflow` - Para Airflow
- `openmetadata_db` - Para OpenMetadata
- `lakefs_db` - Para LakeFS

**Para listar databases:**
```bash
kubectl exec -it -n postgres <postgres-pod-name> -- psql -U postgres -c "\l"
```

---

### 8. **Redis** - Cache e Queue

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

### 9. **Elasticsearch** - Busca e Indexação

**Acesso via kubectl:**
```bash
kubectl port-forward -n elasticsearch svc/elasticsearch-master 9200:9200
# Testar: curl http://localhost:9200
```

**Credenciais:**
- Usuário: `elastic`
- Password: Definido no Secret `elasticsearch-master-credentials` (namespace: `elasticsearch`)

**Funcionalidades:**
- Busca full-text
- Indexação de documentos
- Analytics e agregações
- Usado pelo OpenMetadata para busca de metadados

---

### 10. **Apache Kafka** - Streaming de Eventos

**Acesso via kubectl:**
```bash
# Listar pods Kafka
kubectl get pods -n kafka

# Port-forward para acesso externo (opcional)
kubectl port-forward -n kafka <kafka-pod> 9092:9092
```

**Configuração:**
- 3 Controllers (KRaft mode)
- Tópicos gerenciados via scripts ou ferramentas Kafka
- Usado pelo OpenMetadata para streaming de eventos

**Bootstrap Servers (interno):**
```
kafka-controller-0.kafka-headless.kafka.svc.cluster.local:9092
kafka-controller-1.kafka-headless.kafka.svc.cluster.local:9092
kafka-controller-2.kafka-headless.kafka.svc.cluster.local:9092
```

---

## 📈 Observabilidade

### 11. **Prometheus** - Coleta de Métricas

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

### 12. **Loki** - Agregação de Logs

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

### 13. **Promtail** - Coleta de Logs

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

**Verificar secrets:**
```bash
# Listar SealedSecrets
kubectl get sealedsecrets -A

# Decodificar secret (após ser desencriptado pelo controller)
kubectl get secret <secret-name> -n <namespace> -o yaml
```

**Funcionalidades:**
- Encriptação de secrets antes de commit no Git
- Controller desencripta automaticamente no cluster
- Segurança para credenciais em repositórios Git

---

## 📝 Flux CD - GitOps

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

| Serviço | URL | Autenticação | Observações |
|---------|-----|--------------|-------------|
| Grafana | `http://grafana.vanir-proxmox.duckdns.org` | admin/admin | Mudar senha! |
| Airflow | `http://airflow.vanir-proxmox.duckdns.org` | Secret: `airflow-admin-credentials` | |
| Spark | `http://spark.vanir-proxmox.duckdns.org` | Nenhuma | |
| LakeFS | `http://lakefs.vanir-proxmox.duckdns.org` | Criar conta | NodePort: 30081 |
| MinIO UI | `https://minio-ui.vanir-proxmox.duckdns.org/console/` | Secret: `minio-secrets` | |
| MinIO API | `http://minio-api.vanir-proxmox.duckdns.org` | Secret: `minio-secrets` | Porta 9000 |
| OpenMetadata | Port-forward necessário | Verificar secret | Não exposto via Ingress |
| Prometheus | Port-forward: 9090 | Nenhuma | |
| PostgreSQL | Port-forward: 5432 | Secret no namespace `postgres` | |
| Redis | Port-forward: 6379 | Pode ter auth | |
| Elasticsearch | Port-forward: 9200 | Secret: `elasticsearch-master-credentials` | |

---

## ⚠️ Notas Importantes

1. **Segurança**: Muitos serviços estão configurados com credenciais padrão ou sem autenticação. Para produção, configure:
   - TLS/HTTPS no Ingress
   - Senhas fortes
   - Autenticação adequada

2. **DNS**: O domínio `vanir-proxmox.duckdns.org` precisa estar configurado para apontar para o IP do Ingress Controller (192.168.32.24).

3. **Port-forward**: Para serviços sem Ingress, use `kubectl port-forward` para acesso temporário durante desenvolvimento.

4. **Secrets**: As credenciais estão armazenadas em Secrets do Kubernetes. Use `kubectl get secret` para visualizar (valores base64).

5. **Monitoramento**: O Grafana já está configurado com datasources para Prometheus e Loki. Basta acessar e explorar os dashboards.

---

## 🚀 Próximos Passos Sugeridos

1. Configurar TLS/HTTPS com cert-manager
2. Implementar autenticação SSO (OAuth/OIDC)
3. Configurar backup automatizado dos dados críticos
4. Implementar políticas de rede (NetworkPolicies)
5. Configurar Resource Quotas e Limit Ranges
6. Documentar procedures de disaster recovery

---

**Última atualização**: Dezembro 2024
**Versão do Cluster**: Kubernetes (Talos Linux)
**Flux CD**: GitOps ativo

