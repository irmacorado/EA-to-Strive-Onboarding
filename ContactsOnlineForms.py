# load the necessary packages
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import time
from datetime import datetime
import os
import logging
import json
import time
from urllib.parse import urljoin
from datetime import datetime, timedelta
import sys

# Set parameters
delta  = timedelta(hours =  4) ## Set this to the frequency of your Container Script

#CIVIS enviro variables
van_key = os.environ['VAN_PASSWORD']
strive_key = os.environ['STRIVE_PASSWORD']
campaign_id = os.environ['STRIVE_CAMPAIGN_ID']


# Set EA API credentials
username = 'welcometext'  ## This can be anything
db_mode = '1'    ## Specifying the NGP side of VAN
password = f'{van_key}|{db_mode}' ## Create the password from the key and the mode combined
everyaction_auth = HTTPBasicAuth(username, password)
everyaction_headers = {"headers" : "application/json"}

# Strive parameters
strive_url = "https://api.strivedigital.org/"

##### Set up logger #####
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')



## edits start here
"""
url = "https://api.securevan.com/v4/onlineActionsForms/IAJTrIE6iU-rygF7bXTGeg2"

headers = {"Accept": "application/json"}

response = requests.request("GET", url, headers=headers, auth = everyaction_auth, stream = True)

print(response.text)
"""

url = "https://api.securevan.com/v4/people/100603321"
querystring = {"$expand":"phones,emails"}

headers = {"Accept": "application/json"
          }

response = requests.request("GET", url, headers=headers, params=querystring, auth = everyaction_auth, stream = True)

print(response.text)
