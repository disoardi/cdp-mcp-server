# Available Tools

## Cloudera Manager Tools

| Tool | Description |
|---|---|
| `list_clusters` | List all managed clusters (name, version, health) |
| `list_services` | List services on a cluster |
| `get_service_logs` | Extract service logs with time range filtering |
| `get_alerts` | Get cluster alerts and events |
| `get_service_metrics` | Time-series metrics via tsquery |
| `get_config` | Read service configuration |
| `update_config` | Write service configuration |
| `run_service_command` | Execute async CM command (restart, deploy config, etc.) |
| `get_command_status` | Poll async command status |
| `get_host_status` | Host health, roles, rack info |
| `get_audit_events` | CM audit log |
| `list_datahubs` | Enumerate DataHub clusters |
| `refresh_cluster_map` | Rebuild cluster→CM mapping |
| `get_mgmt_service` | CM Management Service health and role status (Host Monitor, Service Monitor, Alert Publisher, etc.) |

## Registry Tools

| Tool | Description |
|---|---|
| `registry_list` | List registered CM instances |
| `registry_stats` | Registry statistics (total, active, by environment) |
| `registry_add` | Register a new CM instance |
| `registry_deactivate` | Soft-delete a CM instance |
| `registry_update_field` | Update a single field |
| `registry_reload` | Hot-reload registry from backend |

## YARN Tools

| Tool | Description |
|---|---|
| `get_yarn_app` | Get YARN application details, diagnostics, resource usage |
| `list_yarn_apps` | List applications filtered by state/queue/user |
| `get_yarn_queue` | Scheduler queue capacity and active/pending applications |

## Spark History Server Tools

| Tool | Description |
|---|---|
| `get_spark_app` | Spark application summary (duration, executor time) |
| `get_spark_stages` | Stage details including failure reason |
| `list_spark_apps` | List Spark applications |

## HDFS Tools

| Tool | Description |
|---|---|
| `get_namenode_status` | NameNode health (HEALTHY/DEGRADED/CRITICAL), capacity, corrupt/missing blocks |

## Oozie Tools

| Tool | Description |
|---|---|
| `get_oozie_job` | Workflow or coordinator job details with action list |
| `list_oozie_jobs` | List jobs filtered by status/type/user |

## Diagnostic Workflows

**Job failed, why?**
1. `list_yarn_apps` → find the failed `app_id`
2. `get_yarn_app` → read diagnostics
3. `get_spark_stages` → find the failed stage and `failureReason`
4. `get_service_logs` → deep dive into YARN/Spark logs

**HDFS issues?**
1. `get_namenode_status` → check `health_summary`, `corrupt_blocks`, `missing_blocks`
2. `get_alerts` → related alerts on HDFS service

**Resource contention?**
1. `get_yarn_queue` → check `used_capacity` vs `capacity`
2. `list_yarn_apps state=RUNNING` → who is consuming resources
