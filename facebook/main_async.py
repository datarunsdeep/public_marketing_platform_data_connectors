#!/usr/bin/env python
# coding: utf-8

#Copyright 2020 Data Runs Deep Pty Ltd. All rights reserved.

# The current utility is used to fetch insights data at ad level for an account.
# Data is retreived in using facebook async post request. 
# Data is fetched for one week at a daily breakdown
# The request is broken down into multiple chunks to avoid the ongoing facebook insights api performance issue

#import the necessary libraries
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebookads.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
import pandas as pd
import datetime 
from datetime import datetime, timedelta
import dateutil.relativedelta
import json
from google.cloud import bigquery
import os
import time
import configurations as config

client= bigquery.Client()
PROJECT_ID = config.PROJECT_ID
DATASET_ID = config.DATASET_ID
TABLE_ID = config.TABLE_ID
app_id = config.APP_ID 
app_secret = config.APP_SECRET
access_token = config.ACCESS_TOKEN
ad_account_id = config.AD_ACCOUNT_ID
DESTINATION_TABLE_CAMPAIGN = config.DESTINATION_TABLE_CAMPAIGN
DESTINATION_TABLE_ADS = config.DESTINATION_TABLE_ADS
t = 1
# In[103]:
CREDENTIALS_PATH = config.SERVICE_CREDENTIALS 
#service account's authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=CREDENTIALS_PATH

class LibFacebook:
    
        def __init__(self, app_id, app_secret, access_token, ad_account_id):
            FacebookAdsApi.init(app_id, app_secret, access_token)
            self.account = AdAccount(ad_account_id)
                      
        #getting the insights at the "Ads" level
        def get_insights_ads(self,start_date,end_date):            
            
            fields_ads=[
            AdsInsights.Field.date_start,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.clicks,
            AdsInsights.Field.cpc,
            AdsInsights.Field.cpm,
            AdsInsights.Field.frequency,
            AdsInsights.Field.impressions,
            AdsInsights.Field.objective,
            AdsInsights.Field.reach,
            AdsInsights.Field.spend                      
            ]

            params_ads = {
            'level': 'ad',
            'time_range': {'since':str(start_date), 'until':str(end_date)},  
           'time_increment' : 1,  
           'use_account_attribution_setting':True 
                }
            
            #using aync job with a 1 sec wait time
            #be careful about using async = True as that is a reserved keyword for python3
            async_job = self.account.get_insights(fields=fields_ads,params=params_ads, is_async=True)
            async_job.api_get()
            while async_job[AdReportRun.Field.async_status] != 'Job Completed' or async_job[AdReportRun.Field.async_percent_completion] < 100:
                time.sleep(t)
                async_job.api_get()
            time.sleep(t)
            ads_insights = async_job.get_result()
            
            #create an insights object
            obj = ads_insights
            result_arr = []
            for i in obj:
                datadict = {}
                datadict["date"] = i.get("date_start")
                datadict["campaign_id"] = i.get("campaign_id")
                datadict["campaign_name"] = i.get("campaign_name")
                datadict["adset_name"] = i.get("adset_name")
                datadict["ad_name"] = i.get("ad_name")
                datadict["clicks"] = i.get("clicks")
                datadict["cpc"] = i.get("cpc")
                datadict["cpm"] = i.get("cpm")
                datadict["frequency"] = i.get("frequency")
                datadict["impressions"] = i.get("impressions")
                datadict["objective"] = i.get("objective")
                datadict["reach"] = i.get("reach")
                datadict["spend"] = i.get("spend")
            return result_arr


def facebook_ads_data(NewAccount,start_date,end_date):
    
   # Ads level data call 
   # args: account object,start date and end date
    try:
        result_arr = NewAccount.get_insights_ads(start_date,end_date)
        df_ads = pd.DataFrame() #initialise data frame
        df_ads=pd.DataFrame(result_arr) 
        print("get_insights_Ads success")
        return(df_ads)
    except Exception as e:
        print("failed in get_insights_Ads call ")
        print(e)
    

def write_data_to_BQ(df,table_name):  
    # utility to write a dataframe to a bigqquery table
    # args : dataframe, bigquery table name
    print("writing data to:",table_name)
    try:
        job = client.load_table_from_dataframe(df,table_name)
        job.result()
        print("records written to BQ table")
    except Exception as e:
        print("Failed to write data to table")
        print(e)
        
    
if __name__ == '__main__':

    #use timezone and time setting if needed when using time_range  
    fmt = "%Y-%m-%d"
    now_time = datetime.now()
    start_date = now_time - timedelta(7)# 7 days back
    end_date = now_time - timedelta(0)#till today

    # graph = facebook.GraphAPI(access_token)
    NewAccount = LibFacebook(app_id, app_secret, access_token, ad_account_id)
    
   # Get the Facebook ADS data in batches - 
   # I have broken my request into multiple calls to prevent the {500 too much data requested error}
   # You may need to decide on the number of request depending on the size of data requested

    chunks=[]
    while start_date < end_date  :
          new_start_date = start_date 
          new_end_date =  start_date + timedelta(3)
          data = facebook_ads_data(NewAccount,datetime.strftime(new_start_date,fmt),datetime.strftime(new_end_date,fmt))
          chunks.append(data)
          df_ads = pd.concat(chunks)
          start_date = start_date +  timedelta(4)
    print("Total Ads items returned by API",len(df_ads))
    
    # Write data to a big query table
    write_data_to_BQ(df_ads,DESTINATION_TABLE_ADS)
