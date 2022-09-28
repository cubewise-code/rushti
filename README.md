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

* Adjust config.ini to match your TM1 environment
* Create the Tasks.txt file
* Execute the RushTI.py script: 

  Execution for classical type of tasks file with four arguments: 
  - path to tasks.txt 
  - number of maximum workers to run in parallel
  - 'norm' to select the normal execution mode
  - number of retries per process
  
  ```
  python RushTI.py Tasks_type_classic.txt 16 norm 2
  ```

  Example of Tasks_type_classic.txt:
  ```
  instance="tm1srv01" process="load.actuals" pMonth=Jan
  wait
  instance="tm1srv01" process="load.actuals" pMonth=Feb
  instance="tm1srv01" process="load.actuals" pMonth=Mar
  instance="tm1srv01" process="load.actuals" pMonth=Apr
  wait
  instance="tm1srv01" process="load.actuals" pMonth=May
  wait
  instance="tm1srv01" process="load.actuals" pMonth=Jun
  instance="tm1srv01" process="load.actuals" pMonth=Jul
  wait
  instance="tm1srv01" process="load.actuals" pMonth=Aug
  ```

  Or

  Execution for optimized type of tasks file with four arguments: 
  - path to tasks.txt
  - number of maximum workers to run in parallel
  - 'opt' to specify the optimized execution mode
  - number of retries per process
  
  ```
  python RushTI.py Tasks_type_optimized.txt 16 opt 3
  ```

  Example of Tasks_type_optimized.txt:
  ```
  id="1" predecessors="" require_predecessor_success="" instance="tm1srv01" process="load.actuals" pMonth=Jan
  id="2" predecessors="1" require_predecessor_success="1" instance="tm1srv02" process="load.actuals" pMonth=Feb
  id="3" predecessors="1" require_predecessor_success="1" instance="tm1srv01" process="load.actuals" pMonth=Mar
  id="4" predecessors="1" require_predecessor_success="0" instance="tm1srv02" process="load.actuals" pMonth=Apr
  id="5" predecessors="2,3" require_predecessor_success="0" instance="tm1srv01" process="load.actuals" pMonth=May
  id="6" predecessors="4,5" require_predecessor_success="1" instance="tm1srv02" process="load.actuals" pMonth=Jun
  id="7" predecessors="4" require_predecessor_success="0" instance="tm1srv01" process="load.actuals" pMonth=Jul
  id="8" predecessors="6" require_predecessor_success="0" instance="tm1srv02" process="load.actuals" pMonth=Aug
  ```

## Running the tests

No tests yet


## Built With

* [requests](http://docs.python-requests.org/en/master/) - Python HTTP Requests for Humans
* [TM1py](https://github.com/cubewise-code/TM1py) - A python wrapper for the TM1 REST API

## More about TM1py
There are lots of things you can do with TM1py:
* [Upload data from Webservices](https://code.cubewise.com/tm1py-help-content/upload-exchange-rate-from-a-webservice)
* [Data Science with TM1 and Planning Analytics](https://code.cubewise.com/blog/data-science-with-tm1-planning-analytics)
* [Cleaning your instance](https://code.cubewise.com/tm1py-help-content/cleanup-your-tm1-application)

If you are interested you should check the [TM1py-samples](https://github.com/cubewise-code/TM1py-samples).


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
