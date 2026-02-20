# Installation

## Prerequisites

- **Python 3.9+** (if installing via pip)
- **TM1 server** with REST API enabled (address, port, credentials)

## Option 1: pip (Recommended)

```bash
pip install rushti
```

Verify:

```bash
rushti --version
```

## Option 2: Standalone Executable

Download the latest `.exe` from [GitHub Releases](https://github.com/cubewise-code/rushti/releases) — no Python required.

```cmd
rushti.exe --version
```

## Option 3: From Source

```bash
git clone https://github.com/cubewise-code/rushti.git
cd rushti
pip install -e .
```

## Configure Your TM1 Connection

Create a `config/config.ini` file with your TM1 server details:

```ini
[tm1-finance]
address = localhost
port = 12354
user = admin
password = apple
ssl = true
async_requests_mode = True
```

!!! tip
    The section name (`tm1-finance`) is the **instance name** you'll reference in your task files. You can add multiple sections for multiple TM1 servers.

!!! info "Using Planning Analytics as a Service (PAaaS)?"
    Replace address/port/user/password with your IBM Cloud credentials. See the [TM1py documentation](https://github.com/cubewise-code/tm1py) for connection options.

## Set Up Your Working Directory

Set the `RUSHTI_DIR` environment variable to tell RushTI where to find configuration and store all output:

=== "Windows"

    ```cmd
    setx RUSHTI_DIR "C:\rushti\prod"
    ```

=== "Linux / macOS"

    ```bash
    export RUSHTI_DIR=/opt/rushti/prod
    # Add to ~/.bashrc or ~/.zshrc to persist
    ```

Then create the directory structure:

```
RUSHTI_DIR/
├── config/
│   ├── config.ini              # TM1 connection settings
│   ├── settings.ini            # Execution settings (optional)
│   └── logging_config.ini      # Logging configuration (optional)
├── archive/                    # Taskfile JSON snapshots per run (auto-created)
├── data/                       # Stats database (auto-created)
├── logs/                       # Log files (auto-created)
├── checkpoints/                # Resume checkpoints (auto-created)
└── visualizations/             # Dashboards and DAG HTML (auto-created)
```

Only `config/config.ini` is required — the other subdirectories are created automatically.

!!! tip "Without RUSHTI_DIR"
    If `RUSHTI_DIR` is not set, RushTI looks for config files in the current working directory (`./config.ini` or `./config/config.ini`) and writes output relative to the application directory. Setting `RUSHTI_DIR` is recommended for production deployments.

## Next Steps

[:octicons-arrow-right-24: Quick Start — run your first workflow](quick-start.md)
