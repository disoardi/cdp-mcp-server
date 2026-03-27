# cdp-mcp-server

MCP (Model Context Protocol) server for Cloudera Manager / CDP cluster administration and troubleshooting.

Fork of [dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server) — extended with pluggable registry backends (File, Env, Iceberg) and additional service clients (YARN, Spark History Server, HDFS NameNode, Oozie).

## Quick Start (general purpose — no Iceberg required)

### Option 1: FileRegistry (recommended for development)

```bash
# Install
git clone https://github.com/disoardi/cdp-mcp-server.git
cd cdp-mcp-server
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .

# Configure
cp cm_instances.yaml.example cm_instances.yaml
# Edit cm_instances.yaml with your CM credentials

# Run
REGISTRY_BACKEND=file cdp-mcp
```

### Option 2: EnvRegistry (single CM, zero config)

```bash
REGISTRY_BACKEND=env \
  CM_HOST=cm.example.com \
  CM_USERNAME=admin \
  CM_PASSWORD=changeme \
  CM_USE_TLS=false \
  cdp-mcp
```

## Claude Desktop Configuration

### FileRegistry
```json
{
  "mcpServers": {
    "cdp": {
      "command": "/path/to/cdp-mcp-server/.venv/bin/cdp-mcp",
      "env": {
        "REGISTRY_BACKEND": "file",
        "REGISTRY_FILE_PATH": "/path/to/cm_instances.yaml"
      }
    }
  }
}
```

### EnvRegistry (single CM)
```json
{
  "mcpServers": {
    "cdp": {
      "command": "/path/to/cdp-mcp-server/.venv/bin/cdp-mcp",
      "env": {
        "REGISTRY_BACKEND": "env",
        "CM_HOST": "cm.example.com",
        "CM_USERNAME": "admin",
        "CM_PASSWORD": "changeme",
        "CM_USE_TLS": "false"
      }
    }
  }
}
```

## Available Tools

### Cloudera Manager (original dvergari tools)
- `list_clusters` — List all managed DataHub clusters
- `list_services` — List services on a cluster
- `get_service_logs` — Extract service logs with time range filtering
- `get_alerts` — Get cluster alerts and events
- `get_service_metrics` — Time-series metrics via tsquery
- `get_config` / `update_config` — Read and write service configuration
- `run_service_command` / `get_command_status` — Execute async CM commands
- `get_host_status` — Host health and role inventory
- `get_audit_events` — CM audit log
- `list_datahubs` — Enumerate DataHub clusters
- `refresh_cluster_map` — Rebuild cluster→CM mapping

### Registry Management
- `registry_list` — List registered CM instances
- `registry_stats` — Registry statistics
- `registry_add` — Register a new CM instance
- `registry_deactivate` — Deactivate a CM instance
- `registry_update_field` — Update a registry field
- `registry_reload` — Hot-reload registry

### YARN ResourceManager
- `get_yarn_app` — Get YARN application details and diagnostics
- `list_yarn_apps` — List YARN applications with filters
- `get_yarn_queue` — Get YARN scheduler queue capacity/usage

### Spark History Server
- `get_spark_app` — Get Spark application summary
- `list_spark_apps` — List Spark applications
- `get_spark_stages` — Get stage details (useful for debugging slow jobs)

### HDFS NameNode
- `get_namenode_status` — NameNode health, capacity, corrupt/missing blocks

### Oozie
- `get_oozie_job` — Get workflow or coordinator job details
- `list_oozie_jobs` — List Oozie jobs with filters

## Registry Backends

| Backend | Use case | Requires |
|---------|----------|---------|
| `file` | Development, small teams | `cm_instances.yaml` |
| `env` | Single CM, quick tests | env vars `CM_HOST`, `CM_USERNAME`, `CM_PASSWORD` |
| `iceberg` | Production CDP environments | Impala/HiveServer2 + Iceberg table |

## License

Apache License 2.0 — see [LICENSE](LICENSE).
Original work © Davide Vergari.
Modifications © Daniele Isoardi.
