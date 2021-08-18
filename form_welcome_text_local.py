# load the necessary packages
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import time
from datetime import datetime
import os
import json
import time
from urllib.parse import urljoin
from datetime import datetime, timedelta
import sys

# Set parameters
delta  = timedelta(hours =  0.25) ## Set this to the frequency of your Container Script -- 15min?

# Set local environmental variables
van_key = os.environ['VAN_PASSWORD']
strive_key = os.environ['STRIVE_PASSWORD']
campaign_id = os.environ['STRIVE_CAMPAIGN_ID']
van_form = os.environ['VAN_FORM_NAME']
strive_group = os.environ['STRIVE_GROUP_NAME']

# Set EA API credentials
username = 'welcometext'  ## This can be anything
db_mode = '1'    ## Specifying the NGP side of VAN
password = f'{van_key}|{db_mode}' ## Create the password from the key and the mode combined
everyaction_auth = HTTPBasicAuth(username, password)
everyaction_headers = {"headers" : "application/json"}

# Strive parameters
strive_url = "https://api.strivedigital.org/"

##FUNCTIONS
def get_every_action_forms(everyaction_headers, everyaction_auth):
    """
    Prepares the time strings for the EA API end point, creates the URL end point
    and sends a request to the endpoint for an Online Actions submission record, with VanID AND Form Name.
    Returns endpoint with the jobId for the download job to access the requested submissions.
    """

    # Prepare vstrings for Changed Entites API
    max_time = datetime.now()
    min_time = max_time - delta
    max_time_string = max_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    min_time_string = min_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    # EveryAction Changed Entities parameters
    base_everyaction_url = 'https://api.securevan.com/v4/'
    everyaction_job = "changedEntityExportJobs"
    changed_entities_url = urljoin(base_everyaction_url, everyaction_job)

    form_submissions = {
      "dateChangedFrom": 		min_time_string,
      "dateChangedTo" : 		max_time_string,
      "resourceType": 			"ContactsOnlineForms", ##get online forms
      "requestedFields": 		["VanID", "FormName"],
      "excludeChangesFromSelf": "false"
    }

    response = requests.post(changed_entities_url, json = form_submissions, headers = everyaction_headers, auth = everyaction_auth, stream = True)
    jobId = str(response.json().get('exportJobId'))
    everyaction_download_url = f'{changed_entities_url}/{jobId}'
    return everyaction_download_url

def get_export_job(everyaction_download_url, everyaction_headers, everyaction_auth):
    """
    Takes the endpoint for the download job and checks if the downlink is available every 20 seconds. Once the download link is available,
    downloads the data into a data frame. If 1000 seconds have passed and the download link is not available, assume the API has stalled out and
    exit the program to try again the next run.
    """

    timeout = 1000   # [seconds]
    timeout_start = time.time()

    while time.time() < timeout_start + timeout:
    	time.sleep(20) # twenty second delay
    	try:
    		response = requests.get(everyaction_download_url, headers = everyaction_headers, auth = everyaction_auth)
    		downloadLink = response.json().get('files')[0].get('downloadUrl')
    		break
    	except:
    		print("File not ready, trying again in 20 seconds")

    if time.time() == timeout_start + timeout:
    	sys.exit("Export Job failed to download!")
    else:
    	print("Export Job Complete")
    return downloadLink

def prepare_forms_data(FormdownloadLink):
    """
    Takes the downloaded dataframe of forms and
    - Filters Forms data to your form
    Then returns a data frame of submissions to that form.
    """

    df = pd.read_csv(FormdownloadLink)
    # Save a csv for troubleshooting

    # Filter for submissions from your form
    df_form_submissions = df.loc[df['FormName'] == van_form]
    
    if len(df_form_submissions) > 0:
      print(f"{van_form} has {len(df_form_submissions)} submissions. Finding their phone numbers.")
    else:
      sys.exit(f"No submissions for {van_form}. Exiting.")
    return df_form_submissions

"""
We need to find the phone number for each form submission.
First we'll create a function that finds a VANID's phone number.
Then we'll use that function on each VANID that has a form submission to create a dictionary
of best phone numbers for each form submission.
"""

def get_every_action_info(everyaction_headers, everyaction_auth, vanid):
    """
    Gets a persons first name, last name, and phone numbers from their VANID.
    Then it filters all their phone numbers to just those that are SMS opt-ins.
    Then it gets their best phone number (latest phone number).
    Returns a dictionary of someone's best phone number and information for Strive.
    """

    # EveryAction parameters
    base_everyaction_url = 'https://api.securevan.com/v4/'
    everyaction_job = 'people/'
    vanid_str=str(vanid)
    people_url = urljoin(base_everyaction_url, everyaction_job)
    people_url = urljoin(people_url, vanid_str)
    
    querystring = {"$expand":"phones"}
    
    #first get the json response
    response = requests.request("GET", people_url, headers=everyaction_headers, params=querystring, auth = everyaction_auth, stream = True)
    response_json = response.json()
    
    result_dict = {}
    result_dict["vanid"]=vanid
    result_dict["firstName"]=response_json["firstName"]
    result_dict["lastName"]=response_json["lastName"]
    
    # Create a dataframe of all the person's phone numbers to manipulate and filter!
    df_phones = pd.DataFrame(response_json['phones'])

    # Filter for phones that have opted-in
    df_opted_in = df_phones.loc[df_phones['smsOptInStatus'] ==  'Opt-In']

    # Take the most recent phone number
    df_best_phone = df_opted_in.nlargest(1, 'phoneId')

    # Add the best phone number to the final results dictionary
    result_dict['phone'] = df_best_phone['phoneNumber'].iloc[0]

    return result_dict

def create_phones_df(df_form_submissions):
    """
    Takes the form submissions and iterates each submissions' VANID through the `get_every_action_info` function
    to add them to a results data frame for Strive.
    """
    # Create your empty results data frame
    df_for_strive = pd.DataFrame(columns=['vanid','firstName','lastName','phone'], index=[0])

    # Take all the VANIDs from form submissions
    vanids = df_form_submissions['VanID']
  
    # Get each VANID's information
    for vanid in vanids:
      person_dict = get_every_action_info(everyaction_headers, everyaction_auth, vanid)
      
      if len(person_dict) > 0:
        print(f"Information available for (VANID: {vanid})!")
        df_for_strive = df_for_strive.append(person_dict, ignore_index=True) # couldn't figure out how to remove the person_dict headers!
      else:
        print(f"No information or phone number for VANID: {vanid}.")

    if len(df_for_strive) != 0:
        print(f"{len(df_for_strive)} new folks to welcome! Let's send to Strive. They'll handle any deduping.")
    else:
        print(f"No contacts with opted-in phones to welcomed in Strive. Exiting.")

    return df_for_strive

def send_contacts_to_strive(df_for_strive):
    """
    Takes the data frame from the `prepare_forms_data` function and sends each contact
    to Strive and adds them to your group.
    """

    strive_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + strive_key}

    # Iterate through each row and send it to Strive
    for index, row in df_for_strive.iterrows():
      phone_number = row['phone']
      first_name = row['firstName']
      #if pd.isnull(first_name):
        #first_name = "Friend"
      last_name = row['lastName']
      #if pd.isnull(last_name):
        #last_name = "Friend"

      payload = {
				    "phone_number": phone_number,
				    "campaign_id": campaign_id,
				    "first_name": first_name,
				    "last_name": last_name,
				    "opt_in": True,
				      "groups": [
				        {
				          "name": strive_group
				        }
       				]
                    }
      
      response = requests.request("POST", 'https://api.strivedigital.org/members', headers = strive_headers, data = json.dumps(payload))
      
      if response.status_code == 201:
        print(f"Successfully added: {first_name} {last_name}")
      else:
      	print(f"Was not able to add {first_name} {last_name} to Stive. Error: {response.status_code}")

if __name__ == "__main__":
    print("Initiate Export Job")

    everyaction_forms_download_url = get_every_action_forms(everyaction_headers, everyaction_auth)
    FormsdownloadLink = get_export_job(everyaction_forms_download_url, everyaction_headers, everyaction_auth)
    df_form_submissions = prepare_forms_data(FormsdownloadLink)
    
    df_for_strive = create_phones_df(df_form_submissions)    

    send_contacts_to_strive(df_for_strive)
