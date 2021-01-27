#!/usr/bin/env python
# coding: utf-8

# In[5]:


#!/usr/bin/env python
# coding: utf-8


# Copyright 2018 Data Runs Deep Pty Ltd. All rights reserved.


# Install the necessary packages

#Loading the libraries
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime 
from datetime import datetime, timedelta
import configurations as config
from pytz import timezone

KEY_FILE_LOCATION = config.SERVICE_CREDENTIALS
GA_VIEW_ID = config.GA_VIEW_ID
GOOGLE_ANALYTICS_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

# ### Core Logic:
# 
# 1. Call API 
# 2. Parse the results as per your need
# 3. Do necessary pre-processing
# 4. Write the data to a datawarehouse


class GAAPI:
        
 # Function for API service
    def __init__(self):
        """
        Instantiate a connection to the Google Analytics Reporting API
        :return:
        """
        # Load up credentials
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            KEY_FILE_LOCATION, scopes=GOOGLE_ANALYTICS_SCOPES)

    # Build the service object
    # cache_discovery=False is important within a Google Cloud Function,
    # otherwise the library looks for a local credentials file,
    # and eventually crashes
        self.service_ga = build('analyticsreporting', 'v4', credentials=credentials,
                       cache_discovery=False)


# FUNCTION DEFINITIONS:

    def get_report(self,dimension,pg_size,since,until):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
        return self.service_ga.reports().batchGet(
          body={
            'reportRequests': [
            {
              'viewId': GA_VIEW_ID,
              'pageSize': pg_size,
              'dateRanges': [{'startDate': since, 'endDate': until}],
              'dimensions': dimension,
            
              'metrics': [{'expression': 'ga:goal1Completions'}],
#               'segments': [{'segmentId': '<Segment_details>'}],
               'samplingLevel': 'LARGE'
            }
                           ]     
        
            }
          
      ).execute()

#Main function
if __name__ == '__main__':
        
    fmt = "%Y-%m-%d"
    timezonelist = ['Australia/Melbourne'] #depending on the location of your account
    for zone in timezonelist:
        now_time = datetime.now(timezone(zone))
        since = datetime.strftime(now_time - timedelta(7), '%Y-%m-%d') # 7 days back
        until = datetime.strftime(now_time - timedelta(1), '%Y-%m-%d') # till yesterday
    
    #initialise analytics object
    obj = GAAPI()
    
    try:
        ga_dimensions = [{'name':'ga:date'},{'name': 'ga:campaign'}]
        # For Campaign manager and DV360 use below
        #cm_dimensions = [{'name':'ga:date'},{'name': 'ga:dcmClickSitePlacement'}]
        #dv_dimensions = [{'name':'ga:date'},{'name': 'ga:dbmClickLineItem'}]
        pg_size = 100000
        response_ga = obj.get_report(ga_dimensions,pg_size,since,until)
        print(response_ga)
    except Exception as e:
        print(e)

    
# Next steps:
# Data preprocessing (depending on your need)
# Push it to a datawareshouse of your choice (For bigquery: https://github.com/datarunsdeep/public_marketing_platform_data_connectors/blob/1211f0f18e27c4b77b7072e72b6b57204a46dae6/facebook/main.py#L80)




