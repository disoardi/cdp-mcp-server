# cdp-mcp-server

MCP server for **Cloudera Manager / CDP** cluster administration and troubleshooting via AI assistants (Claude, etc.).

Fork of [dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server) — extended with pluggable registry backends and additional service clients.

## Features

- **Pluggable registry**: FileRegistry (YAML), EnvRegistry (env vars), IcebergRegistry (Impala/Iceberg)
- **Auto-discovery**: YARN, Spark History Server, HDFS NameNode, Oozie endpoints discovered from CM at startup
- **19 MCP tools**: cluster management, log extraction, metrics, YARN/Spark/HDFS/Oozie diagnostics
- **No Iceberg required**: use FileRegistry or EnvRegistry for quick setup

## Quick Start

```bash
git clone https://github.com/disoardi/cdp-mcp-server.git
cd cdp-mcp-server
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e .
REGISTRY_BACKEND=env CM_HOST=your-cm CM_USERNAME=admin CM_PASSWORD=pass CM_USE_TLS=false CM_API_VERSION=v41 cdp-mcp
```

See [Installation](installation.md) for detailed setup instructions.
