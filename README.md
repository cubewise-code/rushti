<img src="https://s3-ap-southeast-2.amazonaws.com/downloads.cubewise.com/web_assets/CubewiseLogos/Final+logos_Rushti.png" />

# RushTI

Smooth parallelization of TI Processes with [TM1py](https://code.cubewise.com/tm1py-overview)

## Installing

Install TM1py:

```
pip install TM1py
```

Clone or download the RushTI Repository


## Usage

* Adjust `config.ini` to match your TM1 environment

* Create the `tasks.txt` file

* Execute the `RushTI.py` script using either **named arguments** or **positional arguments**:

### Named Arguments (Recommended)

Use `--help` to see all available options:
```
python RushTI.py --help
```

| Argument | Short | Required | Default | Description |
|----------|-------|----------|---------|-------------|
| `--tasks` | `-t` | Yes | - | Path to the tasks file |
| `--workers` | `-w` | Yes | - | Maximum number of parallel workers |
| `--mode` | `-m` | No | `norm` | Execution mode: `norm` or `opt` |
| `--retries` | `-r` | No | `0` | Number of retries for failed processes |
| `--result` | `-o` | No | `rushti.csv` | Output file for execution results |

**Examples:**
```bash
python RushTI.py --tasks tasks.txt --workers 4
python RushTI.py -t tasks.txt -w 16 -m opt
python RushTI.py --tasks tasks.txt --workers 4 --mode norm --retries 3
python RushTI.py -t tasks.txt -w 8 -m opt -r 2 -o result.csv
```

### Positional Arguments (Legacy)

For backwards compatibility, positional arguments are still supported:

```
python RushTI.py <tasks_file> <workers> [mode] [retries] [result_file]
```

**Examples:**
```bash
python RushTI.py tasks.txt 2                        # Normal mode, 2 workers
python RushTI.py tasks.txt 16 opt                   # Optimized mode, 16 workers
python RushTI.py tasks.txt 4 norm 3                 # Normal mode, 3 retries
python RushTI.py tasks.txt 8 opt 2 result.csv       # Optimized mode, 2 retries, custom result file
```

### The Normal Mode

Parallelizes the TI process execution with n max workers while allowing for execution groups through the `wait` key word

```bash
python RushTI.py -t tasks.txt -w 16 -m norm -r 2 -o results.csv
# or using positional arguments:
python RushTI.py tasks.txt 16 norm 2 results.csv
```

Example of `tasks.txt` in Normal run mode:
```
instance="tm1srv01" process="export.actuals" pMonth=Jan
instance="tm1srv01" process="export.actuals" pMonth=Feb
instance="tm1srv01" process="export.actuals" pMonth=Mar
instance="tm1srv01" process="export.actuals" pMonth=Apr
instance="tm1srv01" process="export.actuals" pMonth=May
instance="tm1srv01" process="export.actuals" pMonth=Jun
wait
instance="tm1srv02" process="import.actuals" pMonth=Jan
instance="tm1srv02" process="import.actuals" pMonth=Feb
instance="tm1srv02" process="import.actuals" pMonth=Mar
instance="tm1srv02" process="import.actuals" pMonth=Apr
instance="tm1srv02" process="import.actuals" pMonth=May
instance="tm1srv02" process="import.actuals" pMonth=Jun
wait
instance="tm1srv02" process="save.data.all"
```

### The Optimized Mode

Parallelizes the TI process execution with n max workers and allows to define individual dependencies between tasks.

Each task is assigned an id. On all tasks you can define predecessors that must have completed before it can run.

```bash
python RushTI.py -t tasks.txt -w 16 -m opt -r 3 -o results.csv
# or using positional arguments:
python RushTI.py tasks.txt 16 opt 3 results.csv
```

Example of `tasks.txt` in Optimized Mode:
```
id="1" predecessors="" require_predecessor_success="" instance="tm1srv01" process="export.actuals" pMonth=Jan
id="2" predecessors="" require_predecessor_success="" instance="tm1srv01" process="export.actuals" pMonth=Feb
id="3" predecessors="" require_predecessor_success="" instance="tm1srv01" process="export.actuals" pMonth=Mar
id="4" predecessors="" require_predecessor_success="" instance="tm1srv01" process="export.actuals" pMonth=Apr
id="5" predecessors="" require_predecessor_success="" instance="tm1srv01" process="export.actuals" pMonth=May
id="6" predecessors="" require_predecessor_success="" instance="tm1srv01" process="export.actuals" pMonth=Jun
id="7" predecessors="1" require_predecessor_success="" instance="tm1srv02" process="load.actuals" pMonth=Jan
id="8" predecessors="2" require_predecessor_success="" instance="tm1srv02" process="load.actuals" pMonth=Feb
id="9" predecessors="3" require_predecessor_success="" instance="tm1srv02" process="load.actuals" pMonth=Mar
id="10" predecessors="4" require_predecessor_success="" instance="tm1srv02" process="load.actuals" pMonth=Apr
id="11" predecessors="5" require_predecessor_success="" instance="tm1srv02" process="load.actuals" pMonth=May
id="12" predecessors="6" require_predecessor_success="" instance="tm1srv02" process="load.actuals" pMonth=Jun
id="13" predecessors="7,8,9,10,11,12" require_predecessor_success="" instance="tm1srv02" process="save.data.all"
```

Please note that the same id can be assigned to multiple tasks.

Both `OPT` and `NORM` now support comments as well as empty lines


## The Result File


RushTI produces a result file with the below format. If no name is provided the file will be named `rushti.csv`. 

Unlike the `rushti.log` file, 
the result file is machine-readable and can be consumed with TI after the execution to gain infomation about the execution.

You can use the result file to raise an alert from TI when not all processes were successful.

| PID   |Process Runs|Process Fails|Start|End|Runtime|Overall Success|
|-------|---------------------|--------------|-------------------|--------------------|--------------|---------------|
| 10332 |8|0|2023-06-26 15:19:28.778457|2023-06-26 15:19:49.160629|0:00:20.382172|True|

## Expandable tasks

Instead of passing individual arguments per task, you can use the expandable assignment operator `*=*` _(default is `=`)_ 
to assign all elements from an MDX result to a parameter. 

RushTI will then register one task per element in the MDX result set.

So this:
```
instance="tm1" process="export.actuals" pMonth*=*"{[Period].[Jan],[Period].[Feb],[Period].[Mar]}"
```

Is equivalent to this:
```
instance="tm1" process="export.actuals" pMonth=Jan
instance="tm1" process="export.actuals" pMonth=Feb
instance="tm1" process="export.actuals" pMonth=Mar
```

You can use expandable assignment on more than one parameter per line.

Expandable tasks can be used both in `NORM` and `OPT` execution mode.

## Logging

Logging can be configured in `logging_config.ini`.
By default, RushTI maintains a small log file  `rushti.log` with the most relevant info's about the execution

The log file is helpful for troubleshooting issues and understanding past executions

```
2023-08-09 14:05:26,506 - 3036 - INFO - RushTI starts. Parameters: ['C:\\RushTI\\RushTI.py', 'tasks.txt', '2'].
2023-08-09 14:05:30,626 - 3036 - INFO - Executing process: '}bedrock.server.wait' with parameters: {'pLogOutput': '1', 'pWaitSec': '6'} on instance: 'tm1srv01'
2023-08-09 14:05:36,633 - 3036 - INFO - Execution successful: Process '}bedrock.server.wait' with parameters: {'pLogOutput': '1', 'pWaitSec': '6'} with 0 retries on instance: tm1srv01. Elapsed time: 0:00:06.005861
...
2023-08-09 14:05:58,682 - 3036 - INFO - Execution successful: Process '}bedrock.server.wait' with parameters: {'pLogOutput': '1', 'pWaitSec': '4'} with 0 retries on instance: tm1srv01. Elapsed time: 0:00:04.008271
2023-08-09 14:06:00,692 - 3036 - INFO - Execution successful: Process '}bedrock.server.wait' with parameters: {'pLogOutput': '1', 'pWaitSec': '6'} with 0 retries on instance: tm1srv01. Elapsed time: 0:00:06.016828
2023-08-09 14:06:00,700 - 3036 - INFO - RushTI ends. 0 fails out of 8 executions. Elapsed time: 0:00:34.191408. Ran with parameters: ['C:\\RushTI\\RushTI.py', 'tasks.txt', '2']
```

## Using the Executable (No Python Required)

If you don't have Python installed, you can use the pre-built Windows executable.

### Download

Download `rushti.exe` from the [GitHub Releases](https://github.com/cubewise-code/rushti/releases) page.

### Setup

The executable requires these configuration files in the **same directory** as `rushti.exe`:

```
your-folder/
├── rushti.exe
├── config.ini          # TM1 server connection settings (required)
├── logging_config.ini  # Logging configuration (required)
└── tasks.txt           # Your task file
```

1. Copy `config.ini` and `logging_config.ini` from this repository to your executable's folder
2. Edit `config.ini` to match your TM1 environment
3. Create your `tasks.txt` file

### Running

Using named arguments:
```cmd
rushti.exe --tasks tasks.txt --workers 4 --mode norm
rushti.exe -t tasks.txt -w 4 -m norm
```

Using positional arguments:
```cmd
rushti.exe tasks.txt 4 norm
```

Check the version or get help:
```cmd
rushti.exe --version
rushti.exe --help
```

### Building Your Own Executable

To build the executable yourself:

1. **Clone the repository and install dependencies**
   ```bash
   git clone https://github.com/cubewise-code/rushti.git
   cd rushti
   pip install -r requirements.txt
   pip install pyinstaller
   ```

2. **Build using the spec file**
   ```bash
   pyinstaller rushti.spec
   ```

3. **Find the executable** in the `dist/` folder

## Built With

[TM1py](https://github.com/cubewise-code/TM1py) - A python wrapper for the TM1 REST API


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
