# RushTI

Smooth parallelization of TI Processes with TM1py

## Installing

Install TM1py:
```
pip install TM1py
```

Clone or download the RushTI Repository


## Usage

* Adjust config.ini to match your TM1 environment
* Create the Tasks.txt file
* Execute the RushTI.py script with two arguments: path to tasks.txt, Number of maximum workers to run in parallel

```
python RushTI.py Tasks.txt 16
```

## Running the tests

No tests yet


## Built With

* [requests](http://docs.python-requests.org/en/master/) - Python HTTP Requests for Humans
* [TM1py](https://github.com/cubewise-code/TM1py) - A python wrapper for the TM1 REST API


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
