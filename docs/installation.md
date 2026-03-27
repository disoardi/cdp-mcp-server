# Installation

## Requirements

- Python 3.11+
- Access to a Cloudera Manager instance

## Install

```bash
git clone https://github.com/disoardi/cdp-mcp-server.git
cd cdp-mcp-server
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Registry Backends

### FileRegistry (recommended for teams)

Create `cm_instances.yaml` from the example:

```bash
cp cm_instances.yaml.example cm_instances.yaml
# Edit with your CM credentials
```

```yaml
instances:
  - host: cm.example.com
    port: 7180
    username: admin
    password: "${CM_PASSWORD}"
    environment_name: dev
    use_tls: false
    verify_ssl: false
    api_version: v41   # adjust to your CM version
```

Run:
```bash
REGISTRY_BACKEND=file cdp-mcp
```

### EnvRegistry (single CM, zero config)

```bash
REGISTRY_BACKEND=env \
  CM_HOST=cm.example.com \
  CM_PORT=7180 \
  CM_USERNAME=admin \
  CM_PASSWORD=changeme \
  CM_USE_TLS=false \
  CM_API_VERSION=v41 \
  cdp-mcp
```

### IcebergRegistry (CDP production)

Original dvergari mode — requires Impala/HiveServer2 with Iceberg table.
See `.env.example` for `IMPALA_*` variables.

## Claude Desktop Configuration

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cdp": {
      "command": "/path/to/cdp-mcp-server/.venv/bin/cdp-mcp",
      "env": {
        "REGISTRY_BACKEND": "env",
        "CM_HOST": "cm.example.com",
        "CM_PORT": "7180",
        "CM_USERNAME": "admin",
        "CM_PASSWORD": "changeme",
        "CM_USE_TLS": "false",
        "CM_API_VERSION": "v41"
      }
    }
  }
}
```

Restart Claude Desktop after saving.

## CM API Version

Different CM versions support different API versions:

| CDH/CDP Version | Max API Version |
|---|---|
| CDH 6.x | v31 |
| CDH 7.0-7.1 | v40-v41 |
| CDP 7.1.7+ | v51+ |

Check your CM version at `http://cm-host:7180/api/version`.
