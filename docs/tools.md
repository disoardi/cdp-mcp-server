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
| `run_service_command` | Execute async CM command (restart, start, stop, deploy config, etc.) |
| `get_command_status` | Poll async command status |
| `get_host_status` | Host health, roles, rack info |
| `get_audit_events` | CM audit log (login, config changes, command executions) |
| `list_datahubs` | Enumerate DataHub clusters |
| `refresh_cluster_map` | Rebuild cluster→CM mapping after failover or new cluster |
| `get_mgmt_service` | CM Management Service health and role status (Host Monitor, Service Monitor, Alert Publisher, etc.) |
| `delete_service` | Delete a stale/orphaned service from a cluster — **irreversible** |
| `delete_role` | Delete a stale/decommissioned role instance — **irreversible** |

!!! warning "Destructive operations"
    `delete_service` and `delete_role` call `DELETE` on the CM API and cannot be undone.
    The target must be in **stopped** state before deletion — CM will return an error otherwise.
    Use `run_service_command` with `command="stop"` first if needed.

## Registry Tools

| Tool | Description |
|---|---|
| `registry_list` | List registered CM instances (passwords excluded) |
| `registry_stats` | Statistics: total, active/inactive count, by environment |
| `registry_add` | Register a new CM instance at runtime |
| `registry_deactivate` | Soft-delete a CM instance (keeps it in registry) |
| `registry_update_field` | Update a single field (e.g. password, port) |
| `registry_reload` | Hot-reload registry from YAML/Iceberg without restart |

## YARN Tools

| Tool | Description |
|---|---|
| `get_yarn_app` | Application details, diagnostics, resource usage and timing |
| `list_yarn_apps` | List applications filtered by state / queue / user |
| `get_yarn_queue` | Scheduler queue capacity and active/pending applications |

## Spark History Server Tools

| Tool | Description |
|---|---|
| `get_spark_app` | Spark application summary (duration, executor time, attempt count) |
| `get_spark_stages` | Stage details including failure reason (truncated to 300 chars) |
| `list_spark_apps` | List Spark applications filtered by status |

## HDFS Tools

| Tool | Description |
|---|---|
| `get_namenode_status` | NameNode health (HEALTHY / DEGRADED / CRITICAL), capacity, corrupt/missing blocks, HA state |

## Oozie Tools

| Tool | Description |
|---|---|
| `get_oozie_job` | Workflow or coordinator job details with action list and YARN app IDs |
| `list_oozie_jobs` | List jobs filtered by status / type / user |

---

## Diagnostic Workflows

### Job failed — why?

1. `list_yarn_apps` with `state=FAILED` → find the `app_id`
2. `get_yarn_app` → read `diagnostics` field
3. `get_spark_stages` with `status=FAILED` → find `failureReason`
4. `get_service_logs` → deep dive into YARN / Spark logs

### HDFS issues?

1. `get_namenode_status` → check `health_summary`, `corrupt_blocks`, `missing_blocks`
2. `get_alerts` with `severity=CRITICAL` → related alerts
3. `get_service_logs` for HDFS → NameNode log details

### Resource contention?

1. `get_yarn_queue` → check `used_capacity` vs `capacity`
2. `list_yarn_apps` with `state=RUNNING` → who is consuming resources
3. `get_service_metrics` → trend over time

### CM internal health?

1. `get_mgmt_service` → Host Monitor, Service Monitor, Alert Publisher status
2. `get_alerts` → any unacknowledged critical alerts
3. `get_host_status` → per-host health and role assignment

### Cleanup — remove stale service or role?

1. `list_services` → confirm the service name
2. `run_service_command` with `command="stop"` → stop the service first
3. `delete_service` → remove it from CM

For a single stale role (e.g. orphaned HiveServer2):

1. `list_services` → identify the service
2. Stop the specific role via `run_service_command` or CM UI
3. `delete_role` with the full role name (e.g. `hive-HIVESERVER2-abc123def456`)
