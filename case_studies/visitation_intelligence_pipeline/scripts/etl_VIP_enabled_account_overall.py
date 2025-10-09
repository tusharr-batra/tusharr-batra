
#importing libraries
import pandas as pd
import time
from datetime import date, timedelta, datetime
import sqlalchemy as a
import boto3
import s3fs
import pandasql
import mysql.connector
import sys
from datetime import date, timedelta, datetime
import json
import awswrangler as wr  
import psycopg2



#adding path to import package file

#Local system path
sys.path.append('/Users/tusharbatra/Projects/tusharr/Script/gt_package')


import package_tusharrbatra as pt

# Reading config file
with open('/Users/tusharbatra/tusharr/Script/gt_package/config.json') as jsonfile: 
    configurations = json.load(jsonfile)
    #print(configurations.keys())
    print("Read Config file successfully")


#Setting up connection with DB(s)

ath_client = boto3.client(configurations['client_name'],region_name = configurations["region"])
s3_client = boto3.client("s3")
region = configurations["region"]
engine_red = a.create_engine(configurations["redshift_connection"])


engine_analyticsAPI = psycopg2.connect(configurations["analyticsAPI_connection"])

output_s3_path='s3://bucket/analytics/vip/stg/VIP_account_enabled_overall/' 


#Reading exclusion list (Tenant/account) - TABLE By Reporting team in Analytics DB


print("running script on : ", date.today())


# Preparing dataset for for Old Accounts

# VIP enabled Accounts (old)

df_account_vip_enable = pd.read_sql("""SELECT * FROM vip_rollback_account_date_dimension WHERE vip_start_date is NOT NULL""", engine_analyticsAPI)


print("Total VIP enabled accounts (old) so far : ",df_account_vip_enable.shape[0])


# Checking latest source and desitination table  refresh date

# Fetching Accounts spent with GT in 2024 or later


query_active_account_current_year = f""" 
          WITH stg_spent_2024_onward AS (
          SELECT 
          account_id,
          SUM (ad_impression) as ad_impression
          
          FROM stg_advertiser_summary
          LEFT JOIN campaign_dimension USING (campaign_id)
          WHERE ad_impression>0 AND  timestamp >= '2024-01-01'
          AND account_id IN {tuple(df_account_vip_enable.account_id)}
          GROUP BY 1
          )

          SELECT 
          account_id,
          account_name,
          tenant_id,
          tenant_name,
          CASE WHEN business_line = 0 THEN 'Managed'
          WHEN business_line = 1 AND business_type IN ( 1, 2 ) THEN 'Reseller'
          WHEN business_line = 1 AND business_type IN ( 3, 5, 6 ) THEN 'Platform'
          WHEN business_line = 1 AND business_type = 4 THEN 'Holding Co'
          ELSE '' END AS business_line,

          CASE WHEN ad_impression >0 THEN True
          ELSE False END AS flag_spent_2024_onward
          
          From campaign_dimension cd
          LEFT JOIN  stg_spent_2024_onward fact USING (account_id)
          WHERE account_id IN {tuple(df_account_vip_enable.account_id)}
          GROUP BY 1,2,3,4,5,6
          """


df_account_spent_2024_onward = pd.read_sql(query_active_account_current_year, engine_red )


# Joining above 2 df

df_account_vip_enable = pd.merge(
    df_account_vip_enable,
    df_account_spent_2024_onward,
    on='account_id', 
    how='inner'       
)



# Add a column for today's date as the first column
df_account_vip_enable.insert(0, 'date_latest_refresh', date.today())


# Storing dataframe to s3


# Pushing the file to s3 in Parquet format

df_account_vip_enable.to_parquet(path=output_s3_path + 'old_account_list_latest_run',compression='gzip', index=False)

print("data is pushed for old accounts in s3 for : ", date.today())


# For New Accounts - Starting after 1st July,2024 and not in A/C Exlclusion list

# A/C exclusion list

df_exclude_account = pd.read_sql("""SELECT * FROM vip_rollback_account_date_dimension """, engine_analyticsAPI)

# New Accounts

query_new_account_default_enable= f"""SELECT
          DISTINCT account_id,
          FIRST_VALUE(start_date) OVER (PARTITION BY account_id ORDER BY start_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS vip_start_date,
          account_name,
          tenant_id,
          tenant_name,
          CASE WHEN business_line = 0 THEN 'Managed'
          WHEN business_line = 1 AND business_type IN ( 1, 2 ) THEN 'Reseller'
          WHEN business_line = 1 AND business_type IN ( 3, 5, 6 ) THEN 'Platform'
          WHEN business_line = 1 AND business_type = 4 THEN 'Holding Co'
          ELSE '' END AS business_line,
          True AS flag_spent_2024_onward
          
          FROM  campaign_dimension cd
          
          WHERE cd.start_date >= '2024-07-01'
          AND account_id NOT IN {tuple(df_exclude_account.account_id)}
          """
          
df_account_new_vip_default_enable = pd.read_sql(query_new_account_default_enable, engine_red )


print("New Accounts enabled on VIP by Default : ", df_account_new_vip_default_enable.account_id.nunique())


# Add a column for today's date as the first column
df_account_new_vip_default_enable.insert(0, 'date_latest_refresh', date.today())


# Storing the dataframe in S3


# Pushing the file to s3 in Parquet format

df_account_new_vip_default_enable.to_parquet(path=output_s3_path + 'new_account_list_latest_run',compression='gzip', index=False)

print("data is pushed for New accounts in s3 for : ", date.today())




print("script run is completed for : ", date.today())





