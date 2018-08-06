<img src="https://s3-ap-southeast-2.amazonaws.com/downloads.cubewise.com/web_assets/tm1py-asset/rushti-noBackground.png" />

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
* Execute the RushTI.py script with two arguments: path to tasks.txt, Number of maximum workers to run in parallel

```
python RushTI.py Tasks.txt 16
```

Example of Tasks.txt:
```
instance="tm1srv01" process="Bedrock.Server.Wait" pWaitSec=1
instance="tm1srv02" process="Bedrock.Server.Wait" pWaitSec=2
instance="tm1srv01" process="Bedrock.Server.Wait" pWaitSec=7
instance="tm1srv01" process="Bedrock.Server.Wait" pWaitSec=3
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
