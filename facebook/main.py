# Copyright 2020 Data Runs Deep.

#!/usr/bin/env python
# coding: utf-8


#import the necessary libraries
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebookads.adobjects.adsinsights import AdsInsights
import pandas as pd
from google.cloud import bigquery
import os
import configurations as config

#import the necessary  parameters
app_id = config.APP_ID
app_secret = config.APP_SECRET
access_token = config.ACCESS_TOKEN
ad_account_id = config.AD_ACCOUNT_ID

client= bigquery.Client()
DESTINATION_TABLE_ADS = config.DESTINATION_TABLE
CREDENTIALS_PATH = config.SERVICE_CREDENTIALS 
#service account's authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=CREDENTIALS_PATH

class LibFacebook:
    
        def __init__(self, app_id, app_secret, access_token, ad_account_id):
            FacebookAdsApi.init(app_id, app_secret, access_token)
            self.account = AdAccount(ad_account_id)
        
        # Getting the insights at "Ad Level".
        # For a full list of available options visit https://developers.facebook.com/docs/marketing-api/insights 
        def get_ads_insights(self):
            # required fields
            fields_ads=[
            AdsInsights.Field.date_start,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.impressions,
            AdsInsights.Field.objective,
            AdsInsights.Field.reach,
            AdsInsights.Field.spend
            ]   
            # params
            params_ads = {
           'level': 'ad', 
             #Yesterday, the 24-hour period between 12:00 AM and 11:59 PM in your account's time zone.
            'date_preset':'yesterday'
                }
            ads_insights = self.account.get_insights(fields=fields_ads, params=params_ads)
            
            #create a result array
            obj = ads_insights
            result_arr = []
            for i in obj:
                datadict = {}
                datadict["date"] = i.get("date_start")
                datadict["campaign_name"] = i.get("campaign_name")
                datadict["campaign_id"] = i.get("campaign_id")
                datadict["ad_id"] = i.get("ad_id")
                datadict["ad_name"] = i.get("ad_name")
                datadict["impressions"] = i.get("impressions")
                datadict["objective"] = i.get("objective")
                datadict["reach"] = i.get("reach")
                datadict["spend"] = i.get("spend")
                result_arr.append(datadict)
            return result_arr
        
                
def facebook_ads_data(NewAccount):
    
    #Ads level data call
    try:
        response_arr = NewAccount.get_ads_insights()
        df_ads = pd.DataFrame() #initialise data frame
        df_ads=pd.DataFrame(response_arr)
        return(df_ads)
    except Exception as e:
        print("Failed in get_ads_insights")
        print(e)

def write_data_to_BQ(df,table_name):    
    my_df = df
    table= table_name
    print("writing data to:",table_name)
    try:
        job = client.load_table_from_dataframe(my_df,table)
        job.result()
        print("records written to BQ table")
    except Exception as e:
        print("Failed to write data to table")
        print(e)
           
            
        
if __name__ == '__main__':
    #graph = facebook.GraphAPI(access_token)
    NewAccount = LibFacebook(app_id, app_secret, access_token, ad_account_id)
    #Get the Facebook insights data
    df_ads = facebook_ads_data(NewAccount)
    #re-arrange column names 
    df_ads =df_ads[['date','campaign_name','campaign_id','ad_id','ad_name','impressions','objective','reach','spend']]
    #Write data to a big query table
    write_data_to_BQ(df_ads,DESTINATION_TABLE_ADS)

