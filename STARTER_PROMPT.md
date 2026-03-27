# STARTER PROMPT — cdp-mcp-server

Prompt per le sessioni Claude Code. Copia il blocco della sessione corrente.

---

## SESSIONE 1 — Fork + Registry pluggable

```
Ciao. Stai lavorando su cdp-mcp-server, un fork di dvergari/cloudera-mcp-server.

LEGGI PRIMA: CLAUDE.md (regole architetturali, struttura, comandi)

Contesto: il repo originale ha cm_client.py e cm_pool.py ottimi ma il registry
richiede Iceberg/Impala obbligatorio. Vogliamo renderlo general-purpose.

Obiettivo questa sessione: registry pluggable senza rompere nulla.

## Setup iniziale
Il repo è già clonato in ~/Progetti/cdp-mcp-server.
Se la struttura non è ancora rinominata:
  mv src/cloudera_manager_mcp src/cdp_mcp
  Aggiorna pyproject.toml: name="cdp-mcp", package dir, entry point

## Task 1 — BaseRegistry ABC
Crea src/cdp_mcp/registry/base.py con:
  class BaseRegistry(ABC):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def get_all(self) -> list[ClouderaManagerSettings]: ...
    def load(self) -> list[ClouderaManagerSettings]: ...
    def register(self, **kwargs) -> str: ...
    def deactivate(self, host: str) -> None: ...
    def update_field(self, host: str, field: str, value: str) -> None: ...
    def list_raw(self, include_inactive: bool = False) -> list[dict]: ...
    def get_stats(self) -> dict: ...
    async def async_load(self) -> list[ClouderaManagerSettings]: ...
    async def async_list_raw(self, ...) -> list[dict]: ...
    async def async_register(self, **kwargs) -> str: ...
    async def async_deactivate(self, host: str) -> None: ...
    async def async_update_field(self, ...) -> None: ...
    async def async_get_stats(self) -> dict: ...

## Task 2 — Sposta IcebergRegistry
Sposta src/cdp_mcp/cm_registry.py → src/cdp_mcp/registry/iceberg.py
Fai ereditare CMRegistry da BaseRegistry. Aggiusta import in server.py e config.py.

## Task 3 — FileRegistry
Crea src/cdp_mcp/registry/file_registry.py con:
  class FileRegistry(BaseRegistry):
    - Legge cm_instances.yaml
    - Supporta interpolazione env vars nei valori: "${ENV_VAR_NAME}"
    - Implementa tutti i metodi ABC (list_raw e get_stats come calcolo in-memory,
      register/deactivate/update_field come modifica del file YAML)
    - Thread-safe (usa threading.RLock per il cache)
    - Async wrappers con run_in_executor come in IcebergRegistry

Schema YAML atteso:
  instances:
    - host: cm.example.com
      port: 7183
      username: admin
      password: "${CM_PASSWORD}"
      environment_name: dev
      use_tls: false
      verify_ssl: false
      api_version: v51
      timeout_seconds: 30
      # opzionale:
      use_knox: false
      # endpoints_override:
      #   yarn_rm: http://rm:8088
      #   spark_hs: http://hs:18088

## Task 4 — EnvRegistry
Crea src/cdp_mcp/registry/env_registry.py con:
  class EnvRegistry(BaseRegistry):
    - Legge da env vars: CM_HOST, CM_PORT (default 7183), CM_USERNAME,
      CM_PASSWORD, CM_ENVIRONMENT (default "default"), CM_USE_TLS (default false),
      CM_VERIFY_SSL (default true), CM_API_VERSION (default v51)
    - Retorna sempre una lista con un solo ClouderaManagerSettings
    - register/deactivate/update_field: raise NotImplementedError con messaggio
      "EnvRegistry is read-only. Use FileRegistry or IcebergRegistry for mutations."

## Task 5 — Factory in config.py
Aggiungi a ServerSettings:
  registry_backend: Literal["iceberg", "file", "env"] = "iceberg"
  registry_file_path: str = "cm_instances.yaml"

Aggiungi funzione:
  def build_registry(settings: ServerSettings) -> BaseRegistry:
    match settings.registry_backend:
      case "iceberg": return IcebergRegistry(ImpalaSettings())
      case "file":    return FileRegistry(settings.registry_file_path)
      case "env":     return EnvRegistry()
      case _: raise ValueError(f"Unknown REGISTRY_BACKEND: {settings.registry_backend}")

## Task 6 — Aggiorna server.py lifespan
Sostituisci la costruzione diretta di CMRegistry con build_registry(server_cfg).

## Verifica
1. uv run cdp-mcp con REGISTRY_BACKEND=env CM_HOST=... deve avviarsi senza errori
2. uv run pytest tests/unit/test_registry.py -v (scrivi il test se non esiste:
   mock FileRegistry con un YAML temporaneo, verifica get_all() ritorna 1 istanza)
3. Smoke test manuale: echo '{"jsonrpc":"2.0","id":1,"method":"tools/call",
   "params":{"name":"registry_stats","arguments":{}}}' | REGISTRY_BACKEND=env
   CM_HOST=fake CM_USERNAME=x CM_PASSWORD=x uv run cdp-mcp

NON implementare i nuovi tool YARN/Spark/HDFS in questa sessione.
```

---

## SESSIONE 2 — Auto-discovery + YARN tools

```
Ciao. Stai lavorando su cdp-mcp-server. Il registry pluggable è completato
(FileRegistry + EnvRegistry funzionanti).

LEGGI PRIMA: CLAUDE.md

Obiettivo: auto-discovery endpoint servizi da CM + 3 tool YARN.

## Task 1 — ServiceEndpoints in cm_pool.py
Aggiungi dataclass:
  @dataclass
  class ServiceEndpoints:
    yarn_rm_url:  Optional[str] = None  # http://rm-host:8088
    spark_hs_url: Optional[str] = None  # http://hs-host:18088
    hdfs_nn_url:  Optional[str] = None  # http://nn-host:9870
    oozie_url:    Optional[str] = None  # http://oozie-host:11000

Aggiungi a CMPool:
  _endpoints: dict[str, ServiceEndpoints]  # cluster_name → endpoints

Aggiungi metodo privato _discover_service_endpoints(cluster_name, client):
  - GET /clusters/{cluster}/services → lista servizi presenti
  - Per ogni servizio trovato:
    YARN → GET /clusters/{cluster}/services/YARN/roles
           → trova RESOURCEMANAGER → hostname
           → GET /clusters/{cluster}/services/YARN/config?view=summary
           → leggi yarn_resourcemanager_webapp_address per la porta (default 8088)
           → costruisci http://{hostname}:{port}
    SPARK_ON_YARN → cerca HISTORYSERVER → porta da history_server_web_port (default 18088)
    HDFS → cerca NAMENODE attivo (healthSummary != BAD) → porta da dfs_http_port (default 9870)
    OOZIE → cerca OOZIE_SERVER → porta da oozie_http_port (default 11000)
  - Se un servizio non è nel cluster → None (non loggare come errore, solo debug)
  - Se discovery fallisce per eccezione → log warning, None (non blocca l'avvio)

Chiama _discover_service_endpoints nel _build_cluster_map dopo la mappatura cluster.

Aggiungi get_endpoints(cluster_name) → ServiceEndpoints al CMPool.

## Task 2 — YarnClient
Crea src/cdp_mcp/clients/yarn_client.py:

class YarnClient:
  def __init__(self, base_url: str, timeout: int = 30): ...
  # Usa httpx.AsyncClient, retry tenacity su TransportError + 503/504
  # Auth: Basic se CM_USERNAME/CM_PASSWORD passati, altrimenti no-auth

  async def get_app(self, app_id: str) -> dict:
    GET /ws/v1/cluster/apps/{app_id}
    Mappa risposta in dict pulito:
      app_id, name, user, queue, state, final_status,
      progress, tracking_url, diagnostics (tronca a 500 char),
      elapsed_time_secs, memory_seconds, vcore_seconds,
      started_time, finished_time, cluster_id
    Se final_status == "FAILED" e diagnostics è vuoto → aggiungi nota:
      "Consulta i log con get_service_logs(service_name='YARN', ...)"

  async def list_apps(self, state: Optional[str] = None,
                      queue: Optional[str] = None,
                      user: Optional[str] = None,
                      limit: int = 20) -> list[dict]:
    GET /ws/v1/cluster/apps con params filtro
    Ordina per startedTime desc, tronca a limit
    Ritorna lista di dict compatti (senza diagnostics per non riempire il contesto)

  async def get_queue(self, queue_name: Optional[str] = None) -> dict:
    GET /ws/v1/cluster/scheduler
    Se queue_name → filtra ricorsivamente la struttura scheduler per trovare la coda
    Ritorna: capacity, usedCapacity, absoluteCapacity, absoluteUsedCapacity,
             numPendingApplications, numActiveApplications, numContainersPending

## Task 3 — Tool in server.py

@mcp.tool()
async def get_yarn_app(cluster_name: str, app_id: str) -> str:
  """
  Get the status and details of a YARN application by ID.
  Returns state, final_status, diagnostics (error message if failed),
  tracking_url, resource usage, and timing information.
  Use this to diagnose why a Spark/MapReduce/Oozie job failed.

  Args:
    cluster_name: DataHub cluster name (use list_clusters to discover).
    app_id:       YARN application ID (e.g. application_1234567890_0001).
  """
  endpoints = _pool.get_endpoints(cluster_name)
  if not endpoints.yarn_rm_url:
    return _dump({"error": f"YARN ResourceManager endpoint not found for cluster '{cluster_name}'. "
                           "Ensure YARN service is running and reachable."})
  client = YarnClient(endpoints.yarn_rm_url)
  return _dump(await client.get_app(app_id))

@mcp.tool()
async def list_yarn_apps(
  cluster_name: str,
  state: Optional[Literal["NEW","NEW_SAVING","SUBMITTED","ACCEPTED",
                           "RUNNING","FINISHED","FAILED","KILLED"]] = None,
  queue: Optional[str] = None,
  limit: int = 20,
) -> str:
  """
  List YARN applications on a cluster, optionally filtered by state and queue.
  Returns a compact summary (no diagnostics). Use get_yarn_app for details
  on a specific application.

  Args:
    cluster_name: DataHub cluster name.
    state:        Filter by application state. Omit for all states.
    queue:        Filter by queue name (partial match).
    limit:        Maximum number of applications to return (default 20).
  """
  # implementazione analoga a get_yarn_app

@mcp.tool()
async def get_yarn_queue(cluster_name: str, queue_name: Optional[str] = None) -> str:
  """
  Get the status of the YARN scheduler queue(s).
  Returns capacity, used capacity, pending and active applications.
  Useful to diagnose resource contention when jobs are waiting.

  Args:
    cluster_name: DataHub cluster name.
    queue_name:   Specific queue to inspect. Omit for the root queue summary.
  """
  # implementazione analoga

## Verifica
1. Unit test con mock httpx per YarnClient:
   - get_app con app FAILED → diagnostics presente
   - get_app con endpoint None → messaggio errore leggibile
   - list_apps → lista ordinata e troncata
   uv run pytest tests/unit/test_yarn_client.py -v

2. Se hai accesso a cluster:
   REGISTRY_BACKEND=env CM_HOST=... uv run cdp-mcp
   Poi chiedi a Claude: "lista le ultime 5 applicazioni YARN sul cluster X"
```

---

## SESSIONE 3 — Spark History Server + HDFS NameNode

```
Ciao. Stai lavorando su cdp-mcp-server. Registry + YARN tools completati.

LEGGI PRIMA: CLAUDE.md

Obiettivo: SparkClient (3 tool) + HdfsClient (1 tool).

## Task 1 — SparkClient
Crea src/cdp_mcp/clients/spark_client.py seguendo il pattern di YarnClient.

async def get_app(self, app_id: str) -> dict:
  GET /api/v1/applications/{app_id}
  Nota: l'app_id per Spark History Server può essere sia l'application ID YARN
  (application_XXXXX_XXXX) sia l'appId Spark (spark-XXXX). Prova entrambi se
  il primo dà 404.
  Ritorna: id, name, attempts (last), startTime, endTime, duration,
           sparkUser, completed, totalExecutorRunTime (dall'ultimo attempt)

async def get_stages(self, app_id: str,
                     status: Optional[str] = None) -> list[dict]:
  GET /api/v1/applications/{app_id}/stages
  Filtra per status se fornito (FAILED, ACTIVE, COMPLETE, PENDING, SKIPPED)
  Per ogni stage: stageId, name, status, numTasks, numFailedTasks,
                  inputBytes, outputBytes, shuffleReadBytes, shuffleWriteBytes,
                  executorRunTime, failureReason (tronca a 300 char)

async def list_apps(self, status: Optional[str] = None,
                    limit: int = 20) -> list[dict]:
  GET /api/v1/applications?status=...&limit=...
  Compatto: id, name, startTime, duration, completed, sparkUser

## Task 2 — HdfsClient
Crea src/cdp_mcp/clients/hdfs_client.py.

async def get_namenode_status(self) -> dict:
  GET /jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState
  + GET /jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus
  Combina in dict unico:
    safe_mode (bool),
    under_replicated_blocks, corrupt_blocks, missing_blocks,
    capacity_total_gb, capacity_used_gb, capacity_remaining_gb,
    capacity_used_pct (calcolato),
    total_files_and_dirs,
    active_namenode_address (da NameNodeStatus.State == "active")
  Aggiungi campo "health_summary":
    "HEALTHY" se safe_mode=false e corrupt=0 e missing=0
    "DEGRADED" se under_replicated > 0
    "CRITICAL" se corrupt > 0 o missing > 0 o safe_mode=true

## Task 3 — Tool in server.py
Aggiungi: get_spark_app, list_spark_apps, get_spark_stages, get_namenode_status
Seguendo lo stesso pattern dei tool YARN: check endpoint → fallback message se None.

## Verifica
uv run pytest tests/unit/test_spark_client.py tests/unit/test_hdfs_client.py -v
uv run pytest tests/unit/ -v  # tutti i test devono passare
```

---

## SESSIONE 4 — Oozie + docs + smoke test reale

```
Ciao. Stai lavorando su cdp-mcp-server. YARN + Spark + HDFS tools completi.

LEGGI PRIMA: CLAUDE.md

Obiettivo: OozieClient + pulizia finale + smoke test su cluster reale.

## Task 1 — OozieClient
Crea src/cdp_mcp/clients/oozie_client.py.

async def get_job(self, job_id: str) -> dict:
  GET /oozie/v2/job/{job_id}?show=info
  Per workflow: id, appName, status, startTime, endTime, createdTime,
                user, group, conf (ometti password/credenziali),
                actions: [{name, type, status, startTime, endTime,
                           errorMessage, externalId (YARN app_id)}]
  Per coordinator: id, appName, status, frequency, timeUnit, nextMaterialize,
                   pauseTime, totalActions, doneActions, failedActions

async def list_jobs(self,
                    status: Optional[str] = None,
                    jobtype: str = "wf",
                    user: Optional[str] = None,
                    limit: int = 20) -> list[dict]:
  GET /oozie/v2/jobs?jobtype=wf&filter=status%3DFAILED&len=20
  Compatto: id, appName, status, startTime, user

## Task 2 — Aggiorna README
Il README esistente (di dvergari) documenta solo IcebergRegistry e Agent Studio.
Aggiungi sezione "Quick Start (general purpose)" con:
  - Setup con FileRegistry (3 comandi)
  - Config Claude Desktop con EnvRegistry
  - Lista tool nuovi (YARN, Spark, HDFS, Oozie)
Non rimuovere la documentazione originale.

## Task 3 — cm_instances.yaml.example
Crea file di esempio con tutti i campi documentati e commenti inline.
Includi sezione endpoints_override commentata per reference.

## Task 4 — Smoke test reale (se cluster disponibile)
Con REGISTRY_BACKEND=file e cm_instances.yaml configurato:

1. list_clusters → deve ritornare i cluster gestiti dal CM
2. list_services (cluster X) → deve listare i servizi
3. list_yarn_apps (cluster X) → deve ritornare app recenti
4. get_yarn_app (cluster X, app_id di una app fallita) → deve avere diagnostics
5. get_namenode_status (cluster X) → deve avere health_summary

Documenta i risultati in un file test_results.md (gitignored).

## Task 5 — Verifica finale
uv run pytest tests/ -v --tb=short
uv run ruff check src/
uv run mypy src/ --ignore-missing-imports

Se tutti i check passano, commit: "feat: registry pluggable + YARN/Spark/HDFS/Oozie tools"
```
