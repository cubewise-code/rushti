<p align="center">
  <img src="https://raw.githubusercontent.com/cubewise-code/rushti/rushti2dot0/docs/assets/images/rushti/RushTi2026_blue.svg" alt="RushTI" width="400" />
</p>

<p align="center">
  <strong>Parallel TI execution engine for IBM Planning Analytics</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/rushti/"><img src="https://img.shields.io/pypi/v/rushti?color=blue" alt="PyPI"></a>
  <a href="https://pypi.org/project/rushti/"><img src="https://img.shields.io/pypi/pyversions/rushti" alt="Python"></a>
  <a href="https://github.com/cubewise-code/rushti/blob/master/LICENSE"><img src="https://img.shields.io/github/license/cubewise-code/rushti" alt="License"></a>
</p>

---

RushTI transforms sequential TurboIntegrator execution into intelligent, parallel workflows. Define task dependencies as a DAG, and RushTI schedules them across multiple workers — starting each task the moment its predecessors complete.

## What's New in 2.0

- **DAG Execution** — True dependency-based scheduling replaces wait-based sequencing
- **JSON Task Files** — Structured format with metadata, settings, and stages
- **Self-Optimization** — EWMA-based learning reorders tasks from historical performance
- **Checkpoint & Resume** — Automatic progress saving with failure recovery
- **Exclusive Mode** — Prevents concurrent runs on shared TM1 servers
- **SQLite Statistics** — Persistent execution history with dashboards and analysis
- **TM1 Integration** — Read tasks from and write results to a TM1 cube
- **100% Backwards Compatible** — Legacy TXT task files work without changes

## Installation

### pip (recommended)

```bash
pip install rushti
```

For the latest beta:

```bash
pip install rushti --pre
```

### uv

```bash
uv pip install rushti
```

### Executable (no Python required)

Download `rushti.exe` from [GitHub Releases](https://github.com/cubewise-code/rushti/releases) — includes all dependencies.

## Quick Start

**1. Configure TM1 connection**

```ini
# config/config.ini
[tm1-finance]
address = localhost
port = 12354
ssl = true
user = admin
password = apple
```

**2. Create a task file**

```json
{
  "version": "2.0",
  "tasks": [
    { "id": "1", "instance": "tm1-finance", "process": "Extract.GL.Data" },
    { "id": "2", "instance": "tm1-finance", "process": "Extract.FX.Rates" },
    {
      "id": "3",
      "instance": "tm1-finance",
      "process": "Transform.Currency",
      "predecessors": ["1", "2"]
    },
    {
      "id": "4",
      "instance": "tm1-finance",
      "process": "Build.Reports",
      "predecessors": ["3"]
    }
  ]
}
```

**3. Validate and run**

```bash
rushti tasks validate --tasks daily-refresh.json --skip-tm1-check
rushti run --tasks daily-refresh.json --max-workers 4
```

## Documentation

Full documentation is available at **[cubewise-code.github.io/rushti/docs](https://cubewise-code.github.io/rushti/docs/)**

- [Installation](https://cubewise-code.github.io/rushti/docs/getting-started/installation/)
- [Quick Start](https://cubewise-code.github.io/rushti/docs/getting-started/quick-start/)
- [Task Files](https://cubewise-code.github.io/rushti/docs/getting-started/task-files/)
- [CLI Reference](https://cubewise-code.github.io/rushti/docs/advanced/cli-reference/)
- [Settings Reference](https://cubewise-code.github.io/rushti/docs/advanced/settings-reference/)

## Website

Visit **[cubewise-code.github.io/rushti](https://cubewise-code.github.io/rushti/)** for interactive demos, feature overviews, and architecture visualizations.

## Links

- [GitHub](https://github.com/cubewise-code/rushti)
- [PyPI](https://pypi.org/project/rushti/)
- [Issues](https://github.com/cubewise-code/rushti/issues)
- [Changelog](https://github.com/cubewise-code/rushti/releases)

## Built With

[TM1py](https://github.com/cubewise-code/TM1py) — Python interface to the TM1 REST API

## License

MIT — see [LICENSE](LICENSE) for details.
