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

* Execute the `RushTI.py` script: with 2 to 5 arguments, e.g.:

  -`python RushTI.py tasks.txt 2 ` 
*Executes rushti in normal mode based on tasks.txt with 2 threads*

  -`python RushTI.py tasks.txt 16 OPT`
*Executes rushti in optimized mode based on tasks.txt with 16 threads*

  -`python RushTI.py tasks.txt 4 NORM 3`
*Executes rushti in normal mode based on tasks.txt with 4 threads. Allow 3 retries per task*

  -`python RushTI.py tasks.txt 8 opt 2 result.csv`
*Executes rushti in normal mode based on tasks.txt with 8 threads. Allow 2 retries per task. Write result file to result.csv*

### The Normal Mode

Parallelizes the TI process execution with n max workers while allowing for execution groups through the `wait` key word

```
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


```
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

## Need a .exe version of RushTI?

Download RushTI and Use PyInstaller to create the .exe file

## Built With

[TM1py](https://github.com/cubewise-code/TM1py) - A python wrapper for the TM1 REST API


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
