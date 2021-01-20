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
DESTINATION_TABLE_CAMPAIGN = config.DESTINATION_TABLE
CREDENTIALS_PATH = config.SERVICE_CREDENTIALS 
#service account's authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=CREDENTIALS_PATH

class LibFacebook:
    
        def __init__(self, app_id, app_secret, access_token, ad_account_id):
            FacebookAdsApi.init(app_id, app_secret, access_token)
            self.account = AdAccount(ad_account_id)
        
        #getting the campaign insights at "Campaign Level"
        def get_campaign_insights_campaign(self):
            # required fields
            fields_campaign=[
            AdsInsights.Field.date_start,
#             AdsInsights.Field.campaign_name,
#             AdsInsights.Field.campaign_id,
            AdsInsights.Field.impressions,
            AdsInsights.Field.objective,
            AdsInsights.Field.reach,
            AdsInsights.Field.spend
            ]   
            # params
            params_campaign = {
           'level': 'campaign', 
             #Yesterday, the 24-hour period between 12:00 AM and 11:59 PM in your account's time zone.
            'date_preset':'yesterday'
                }
            campaign_insights = self.account.get_insights(fields=fields_campaign, params=params_campaign)
            
            #create a result array
            obj = campaign_insights
            result_arr = []
            for i in obj:
                datadict = {}
                datadict["date"] = i.get("date_start")
#                 datadict["campaign_name"] = i.get("campaign_name")
#                 datadict["campaign_id"] = i.get("campaign_id")
                datadict["impressions"] = i.get("impressions")
                datadict["objective"] = i.get("objective")
                datadict["reach"] = i.get("reach")
                datadict["spend"] = i.get("spend")
                result_arr.append(datadict)
            return result_arr
        
                
def facebook_campaign_data(NewAccount):
    
    #Campaign level data call
    try:
        response_arr = NewAccount.get_campaign_insights_campaign()
        df_campaign = pd.DataFrame() #initialise data frame
        df_campaign=pd.DataFrame(response_arr)
        return(df_campaign)
    except Exception as e:
        print("Failed in get_campaign_insights_Campaign call ")
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
    # graph = facebook.GraphAPI(access_token)
    NewAccount = LibFacebook(app_id, app_secret, access_token, ad_account_id)
    #Get the Facebook CAMPAIGN data
    df_campaign = facebook_campaign_data(NewAccount)
    # re-arrange column names 
    df_campaign =df_campaign[['date','impressions','objective','reach','spend']]
#   Write data to a big query table
    write_data_to_BQ(df_campaign,DESTINATION_TABLE_CAMPAIGN)

