

import pandas as pd
from datetime import date, timedelta, datetime
import sqlalchemy as a
import boto3
import awswrangler as wr
from io import BytesIO
import os
import sys
import psycopg2
import mysql.connector
import pyarrow.parquet as pq
import time
import json
import copy


# Reading Configuration file

with open("/home/tusharbatra/scripts/vip/Groundtruth/config.json", "r") as jsonfile:
    configurations = json.load(jsonfile)
    print(configurations.keys())
    print("Read successful")

# Setting up Connections

ath_client = boto3.client('athena',region_name = configurations["region"])
s3_client = boto3.client("s3")
bucket =configurations["bucket"]
workgroup = configurations["workgroup"]


## Connecting to DataBases

engine_redshift = a.create_engine(configurations["cxn_red"])
engine_analytics = a.create_engine(configurations["conn_analytics"])
connection_xad = a.create_engine(configurations["conn_xad"])
connection_cent = a.create_engine(configurations["conn_central"])

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


#source table name declaration

offline = configurations["offline_table"]
online = configurations["online_table"]
true_visits = configurations["true_visits"]
stg_adv = configurations["stg_table"]
status_log = configurations["status_log"]
sightings = configurations["sightings_table"]

# Defining Functions

# Fetching latest date available in the source tables

#function returns the last updated date  - True Visits table
# US
def lastUpdated_truevisits_us():
    query = pd.read_sql(f'''SELECT max(date) as a
                        FROM (SELECT CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) AS date
                                    , count(*) as total_hours
                            FROM {status_log}
                            
                            WHERE `key` = '/visit_store/true_visit_etl/us'
                            AND hour IS NOT NULL
                            AND CAST(timestamp  as date) > (CURDATE()-INTERVAL 31 DAY )
                            GROUP BY  1
                            HAVING count(*) = 24
                            ) as base ''',connection_cent)

    last_updated_date = query._get_value(0,'a')
    print("Last Updated Date for True Visits Data - US : ",last_updated_date)
    return last_updated_date

# CA
def lastUpdated_truevisits_ca():
    query = pd.read_sql(f'''SELECT max(date) as a
                        FROM (SELECT CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) AS date
                                    , count(*) as total_hours
                            FROM {status_log}
                            
                            WHERE `key` = '/visit_store/true_visit_etl/ca'
                            AND hour IS NOT NULL
                            AND CAST(timestamp  as date) > (CURDATE()-INTERVAL 31 DAY )
                            GROUP BY  1
                            HAVING count(*) = 24
                            ) as base ''',connection_cent)

    last_updated_date = query._get_value(0,'a')
    print("Last Updated Date for True Visits Data  - Canada: ",last_updated_date)
    return last_updated_date

#function returns the last updated date for Sightings table
def lastUpdated_sightings():
    query = pd.read_sql(f'''SELECT 
             max(date) as a 
          FROM 
            (SELECT 
          date 
          FROM (SELECT 
              year, 
              month, 
              day, 
              hour, 
              CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS   CHAR(50))) AS DATE) 
          AS date
          FROM 
            {status_log}
          WHERE 
            `key` IN ('/staging/insights-etl/vhs-orc/us/exchange', '/staging/insights-etl/vhs-orc/us/display_dr')
          AND CAST(concat(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) > (CURDATE()-INTERVAL 31 day)
           ) a
         ) a''',connection_xad)

    last_updated_date = query._get_value(0,'a')
    print("Last Updated Date for Sightings Data : ",last_updated_date)
    return last_updated_date

#function returns the last updated date for Store Visitation ORC table

def lastUpdated_storevisitationorc():
    query = pd.read_sql(f'''SELECT 
          max(date) as a 
          FROM 
            (SELECT 
               date 
          FROM (SELECT 
              year, 
              month, 
              day, 
              hour, 
              CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS   CHAR(50))) AS DATE) 
          AS date
        FROM 
             {status_log}
        WHERE 
           `key` = '/enigma/user_store/store_visitation_performance_summary/generate/cmb'
        AND CAST(concat(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) > (CURDATE()-INTERVAL 31 day)
        ) a
 ) a''',connection_cent)

    last_updated_date = query._get_value(0,'a')
    print("Last Updated Date for Store Visitation ORC: ",last_updated_date)
    return last_updated_date

#function returns the last updated date for offline attribution table

def lastUpdated_offlineattribution():
    query = pd.read_sql(f'''SELECT 
             max(date) as a 
          FROM 
            (SELECT 
          date 
          FROM (SELECT 
              year, 
              month, 
              day, 
              hour, 
              CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS   CHAR(50))) AS DATE) 
          AS date
          FROM 
             {status_log}
          WHERE 
            `key`='offline-attribution'
          AND CAST(concat(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) > (CURDATE()-INTERVAL 31 day)
           ) a
         ) a''',connection_cent)

    last_updated_date = query._get_value(0,'a')
    print("Last Updated Date for Offline Attribution: ",last_updated_date)
    return last_updated_date


#function returns the last updated date for stg_advertiser_summary
#parameters - none
def lastUpdated_advertiser_summary():
    query = pd.read_sql(f'''SELECT 
                    max(date) as a 
                    FROM (SELECT date 
                  FROM (SELECT 
                      year, 
                      month, 
                      day, 
                      hour, 
                      CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE)
                  AS date
                  FROM 
                     {status_log}
                  WHERE 
                     `key` = '/enigma/advreport/stg/insert/stg_advertiser_summary'
                  AND 
                     CAST(concat(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) 
                  AS DATE) > (CURDATE()-INTERVAL 31 day)
                        ) a
                 ) a''',connection_xad)

    last_updated_date = query._get_value(0,'a')
    print("Last Updated Date for Stg Advertiser Summary : ",last_updated_date)
    return last_updated_date

#to delete all objects in a s3 path
def delete_all_objects_from_s3_folder(prefix,delete_all_files):
    # First we list all files in folder
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response.keys():
        files_in_folder = response["Contents"]
        files_to_delete = []
        # We will create Key array to pass to delete_objects function
        if(delete_all_files == 1):
            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})
        else:
            for f in files_in_folder:
                if (('.txt' in f["Key"]) or ('.metadata' in f["Key"]) or ('.txt.metadata' in f["Key"]) or ('.csv' in f["Key"])): 
                    files_to_delete.append({"Key": f["Key"]})
        # This will delete all files in a folder
        print(files_to_delete)
        response = s3_client.delete_objects(Bucket=bucket, Delete={"Objects": files_to_delete})
        print("***Deleted files:***",files_to_delete)

#function removes all the pre-existing files in a particular s3 location to avoid duplicacy
#parameters - s3_path   
def delete_if_data_exists(s3_path_delete_all):
    command = 'aws s3 rm '+s3_path_delete_all+' --recursive --exclude "*" --include "*.csv"'
    os.system(command)

#function runs the query in athena through boto3 and saves the output in s3
#parameters - query, database, s3_path
def running_query(query, database, s3_output):
    response = ath_client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
            'Database': database
            },
        ResultConfiguration = {
            'OutputLocation': s3_output,
            },
        WorkGroup=workgroup
        )
    print('\t\t\tQuery Execution ID: ' + response['QueryExecutionId'])
    
    query_status = 'QUEUED'
    print("\t\t\tQuery is queued")
    while(query_status in ['QUEUED','RUNNING']):
        query_status = ath_client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State']
    print("\t\t\tFinal query status: ",query_status)
    print("\t\t\tData saved at the location:", s3_output)

#function removes all the unnecessary files in a particular s3 location
#parameters - s3_path
def remove_file(remove_path):
    command1 = 'aws s3 rm '+remove_path+' --recursive --exclude "*" --include "*.metadata"'
    command2 = 'aws s3 rm '+remove_path+' --recursive --exclude "*" --include "*.txt"'
    command3 = 'aws s3 rm '+remove_path+' --recursive --exclude "*" --include "*.txt.metadata"'

    os.system(command1)
    os.system(command2)
    os.system(command3)

#function to update the partitions in the table location 
#parameters - table_name, s3_path, database
def loading_partitions_table(table,s3_path,database):
    time.sleep(60)
    remove_file(s3_path)
    prefix_s3 = s3_path.split('//')[1]
    prefix_s3 = prefix_s3.replace("bucket",'/').split('//')[1]
#     delete_all_objects_from_s3_folder(prefix_s3,0)
    print("\t\tLoading partitions for table:",table)
    query= '''MSCK REPAIR TABLE {}'''.format(table)
    query_status='QUEUED'
    running_query(query,database,s3_path)
    time.sleep(30)
    remove_file(s3_path)

#function to return year,month and day for a particular date
#parameters - date
def calculate_ymd(date_ymd):
    d = date_ymd.day
    if(d < 10):
        d = '0' + str(d)
    else:
        d = str(d)
    m = date_ymd.month
    if(m < 10):
        m = '0' + str(m)
    else:
        m = str(m)
    y = str(date_ymd.year)
    return y,m,d


def campaign_monitor(current_date_running,path_data,database,table_name):
    
 #calculate the number of days the function should run
    max_date = wr.athena.read_sql_query(sql = f'''Select max(date) as max_date from {table_desitnation}''', database = database, workgroup = workgroup)
    max_date = max_date['max_date'][0]
       
    current_date_running = copy.deepcopy(current_date_running)
    num_days = (current_date_running - max_date).days
    
    
    print("*** Max Date updated for Campaign Monitor", max_date, " ***")   
    
    print("*** No of days to backfill is", num_days, "days ***")
    
    
    if num_days > 0:
        for i in range(num_days):
            #splits year month and date from the given date to partition the s3 path
            year,month,day = calculate_ymd(current_date_running)
            s3_path = path_data + 'y={}/m={}/d={}/'.format(year,month,day)
            s3_output = 'analytics/vip/stg/campaign_monitor/' + 'y={}/m={}/d={}/'.format(year,month,day)
            print(s3_output)
            prefix_s3_temp = s3_path.split('//')[1]
            prefix_s3 = prefix_s3_temp.replace("bucket",'/').split('//')[1]
            query_campaign = f'''select 
                  a.timestamp as date, 
                  a.campaign_id, 
                  a.adgroup_id, 
                  c.device_type, 
                  d.total_budget, 
                  sum(a.ad_impression) as impressions 
                from 
                  {stg_adv} a 
                  left join public.adgroup_dimension b on a.adgroup_id = b.adgroup_id 
                  left join ops.budget_dimension_updated d on a.adgroup_id = d.adgroup_id 
                  LEFT JOIN (
                    SELECT 
                      adgroup_id, 
                      CASE WHEN constraints IN (
                        'app', 'app,web', 'web,app', 'web'
                      ) THEN 'mobile' WHEN constraints = 'desktop' THEN 'desktop' WHEN constraints = 'ott' THEN 'ott' WHEN constraints IN ('ctv', 'ott,ctv', 'ctv,ott') THEN 'ctv' ELSE null END as device_type 
                    FROM 
                      public.viewability_dimension 
                    WHERE 
                      target_type = 6
                  ) as c on b.adgroup_id = c.adgroup_id 
                where 
                  a.ad_impression > 0 
                  and b.adgroup_type = 'DISPLAY' 
                  and b.del = 0 
                  and b.adgroup_status = 'ACTIVE' 
                  and b.enable_store_visitation = 1 
                  and a.timestamp = '{current_date_running}' 
                group by 
                  1, 
                  2, 
                  3, 
                  4, 
                  5
                '''
            print(query_campaign)
            df_campaign = pd.read_sql(query_campaign,engine_redshift)
            print(df_campaign)
            if df_campaign.empty:
                print('Empty Dataframe')
            else:
                delete_all_objects_from_s3_folder(prefix_s3,0)
                #delete_if_data_exists(s3_path)
                bucket = bucket
                path = s3_output + 'df_campaign.parquet'
                parquet_buffer = BytesIO()
                df_campaign.to_parquet(parquet_buffer)
                s3_resource = boto3.resource('s3')
                #stores the df into the desired s3 path
                s3_resource.Object(bucket,path).put(Body=parquet_buffer.getvalue())
                print('*** The data is stored for campaign_monitor to this ',s3_path, ' path successfully ***')
                current_date_running = current_date_running - timedelta(1)

        loading_partitions_table(table_name,path_data, database)
        print("\t** Process Completed for Campaign Monitor **")
        
    else:
        print("*** The table for Campaign Monitor is upto date *** ")


# In[13]:

def truevisits_us(current_date_running,path_data,database,table_name):
    
    #calculate the number of days the function should run
    max_date = wr.athena.read_sql_query(sql = '''Select cast(max(date) as date) as max_date from aditya.stg_vip_truevisits''', database = database, workgroup = workgroup)
    max_date = max_date['max_date'][0]
    current_date_running = copy.deepcopy(current_date_running)
    num_days = (current_date_running - max_date).days
    
    print("*** Max Date updated for true visits (US)", max_date, " ***")
    print("*** No of days to backfill is", num_days, "days ***")
    
    if num_days > 0:
        for i in range(num_days):
            #splits year month and date from the given date to partition the s3 path
            year,month,day = calculate_ymd(current_date_running)
            s3_output = path_data + 'y={}/m={}/d={}/'.format(year,month,day)
            s3_path = 'analytics/vip/stg/sightings_truevisits/' + 'y={}/m={}/d={}/'.format(year,month,day)
            prefix_s3_temp = s3_output.split('//')[1]
            prefix_s3 = prefix_s3_temp.replace("bucket",'/').split('//')[1]
            print(prefix_s3)
            dfs = []
            h = '00'
            for i in range(24):
                date = str(current_date_running)
                h = str(h).zfill(2)
                query_truevisits = f'''WITH stg_base as 
                    (SELECT          Uid,
                                     poi_hash,
                                     Unnested_pub_id.pub_id as pub_id,
                                     pd.is_data_partner,
                                     hour_local as dt

                    FROM {true_visits} tv  
                    CROSS JOIN UNNEST (tv.pub_id) AS Unnested_pub_id (pub_id)

                    inner join dimensions.publisher_dimension pd
                    on Unnested_pub_id.pub_id = publisher_id

                    WHERE is_employee = 0
                    AND is_open = 1
                    AND agg_proximitiy_mode <> 'NA' 
                    and hour_local = '{date} {h}'
		    AND country = 'us'
                    GROUP BY 1,2,3,4,5

                    )

                    ,stg_true_visit_total 
                    as (
                    SELECT uid, 
                          poi_hash,
                          dt,
                          Count(*) OVER () AS true_visit_total
                    FROM stg_base
                    GROUP BY 1,2,3
                    )
                    ,
                    stg_true_visit_by_partner
                    as(
                    SELECT uid, 
                          poi_hash
                          ,is_data_partner
                          ,dt
                     ,sum(CASE WHEN is_data_partner = 0 THEN 1 ELSE 0 END ) OVER () as true_visit_ap
                     ,sum(CASE WHEN is_data_partner = 1 THEN 1 ELSE 0 END ) OVER () as true_visit_dp    
                    FROM stg_base
                    GROUP BY 1,2,3,4
                    )
                    ,temp_agg_true_visit_total 
                    as (
                    SELECT dt as date,
                          cast(avg(true_visit_total) as bigint) as true_visit_total
                          FROM stg_true_visit_total
                        GROUP BY 1
                    )
                    ,temp_agg_true_visit_by_partner
                    as (
                    SELECT dt as date,
                          cast(avg(true_visit_ap) as bigint) as true_visit_ap,
                          cast(avg(true_visit_dp) as bigint) as true_visit_dp
                          FROM stg_true_visit_by_partner
                        GROUP BY 1
                    )

                    SELECT date,
                        a.true_visit_total,
                        b.true_visit_ap,
                        b.true_visit_dp

                    FROM temp_agg_true_visit_total a
                    LEFT JOIN temp_agg_true_visit_by_partner b
                    USING (date)'''
                print('*** The query is running for True Visits - US ***')
                #print(query_truevisits)
                df_visits = wr.athena.read_sql_query(sql = query_truevisits, database = database, workgroup = workgroup)
                dfs.append(df_visits)
                h = str(int(h)+1)
            final_df = pd.concat(dfs, ignore_index=True)
            print(dfs)
            print(final_df)
            final_df.date = final_df.date.apply(lambda x:x.split(' ')[0])
            final_df = final_df.groupby('date').sum().reset_index()
            print(final_df)

            #print(query_truevisits)
            #delete_if_data_exists(s3_output)
            delete_all_objects_from_s3_folder(prefix_s3,0)                                  
            print("*** The path that the data is stored ", s3_output, "***")
            #runs the query in athena
            bucket = 'bucket' 
            path = s3_path + 'final_df.parquet'
            parquet_buffer = BytesIO()
            final_df.to_parquet(parquet_buffer)
            s3_resource = boto3.resource('s3')
            #stores the df into the desired s3 path
            s3_resource.Object(bucket,path).put(Body=parquet_buffer.getvalue())

            print("*** The query successfully completed for True Visits (US) for ", current_date_running)
            current_date_running = current_date_running - timedelta(1)

        #msck repair
        loading_partitions_table(table_name,path_data, database)
        print("\t** Process Completed for True Visits (US) **")
        
    else:
        print("*** The table for True visits is upto date *** ")

### True visits for Canada (CA)

def truevisits_ca(current_date_running,path_data,database,table_name):
    
    #calculate the number of days the function should run
    max_date = wr.athena.read_sql_query(sql = '''Select cast(max(date) as date) as max_date from aditya.stg_vip_truevisits_canada''', database = database, workgroup = workgroup)
    max_date = max_date['max_date'][0]
    current_date_running = copy.deepcopy(current_date_running)
    num_days = (current_date_running - max_date).days
    
    print("*** Max Date updated for true visits (Canada) : ", max_date, " ***")
    print("*** No of days to backfill is", num_days, "days ***")
    
    if num_days > 0:
        for i in range(num_days):
            #splits year month and date from the given date to partition the s3 path
            year,month,day = calculate_ymd(current_date_running)
            s3_output = path_data + 'y={}/m={}/d={}/'.format(year,month,day)
            s3_path = 'analytics/vip/stg/true_visit_canada/' + 'y={}/m={}/d={}/'.format(year,month,day)
            prefix_s3_temp = s3_output.split('//')[1]
            prefix_s3 = prefix_s3_temp.replace("bucket",'/').split('//')[1]
            print(prefix_s3)
            dfs = []
            h = '00'
            for i in range(24):
                date = str(current_date_running)
                h = str(h).zfill(2)
                query_truevisits = f'''WITH stg_base as 
                    (SELECT          Uid,
                                     poi_hash,
                                     Unnested_pub_id.pub_id as pub_id,
                                     pd.is_data_partner,
                                     hour_local as dt

                    FROM {true_visits} tv  
                    CROSS JOIN UNNEST (tv.pub_id) AS Unnested_pub_id (pub_id)

                    inner join dimensions.publisher_dimension pd
                    on Unnested_pub_id.pub_id = publisher_id

                    WHERE is_employee = 0
                    AND is_open = 1
                    AND agg_proximitiy_mode <> 'NA' 
                    and hour_local = '{date} {h}'
		    AND country = 'ca'
                    GROUP BY 1,2,3,4,5

                    )

                    ,stg_true_visit_total 
                    as (
                    SELECT uid, 
                          poi_hash,
                          dt,
                          Count(*) OVER () AS true_visit_total
                    FROM stg_base
                    GROUP BY 1,2,3
                    )
                    ,
                    stg_true_visit_by_partner
                    as(
                    SELECT uid, 
                          poi_hash
                          ,is_data_partner
                          ,dt
                     ,sum(CASE WHEN is_data_partner = 0 THEN 1 ELSE 0 END ) OVER () as true_visit_ap
                     ,sum(CASE WHEN is_data_partner = 1 THEN 1 ELSE 0 END ) OVER () as true_visit_dp    
                    FROM stg_base
                    GROUP BY 1,2,3,4
                    )
                    ,temp_agg_true_visit_total 
                    as (
                    SELECT dt as date,
                          cast(avg(true_visit_total) as bigint) as true_visit_total
                          FROM stg_true_visit_total
                        GROUP BY 1
                    )
                    ,temp_agg_true_visit_by_partner
                    as (
                    SELECT dt as date,
                          cast(avg(true_visit_ap) as bigint) as true_visit_ap,
                          cast(avg(true_visit_dp) as bigint) as true_visit_dp
                          FROM stg_true_visit_by_partner
                        GROUP BY 1
                    )

                    SELECT date,
                        a.true_visit_total,
                        b.true_visit_ap,
                        b.true_visit_dp

                    FROM temp_agg_true_visit_total a
                    LEFT JOIN temp_agg_true_visit_by_partner b
                    USING (date)'''
                print('*** The query is running for True Visits - Canada ***')
                #print(query_truevisits)
                df_visits = wr.athena.read_sql_query(sql = query_truevisits, database = database, workgroup = workgroup)
                dfs.append(df_visits)
                h = str(int(h)+1)
            final_df = pd.concat(dfs, ignore_index=True)
            print(dfs)
            print(final_df)
            final_df.date = final_df.date.apply(lambda x:x.split(' ')[0])
            final_df = final_df.groupby('date').sum().reset_index()
            print(final_df)

            #print(query_truevisits)
            #delete_if_data_exists(s3_output)
            delete_all_objects_from_s3_folder(prefix_s3,0)                                  
            print("*** The path that the data is stored ", s3_output, "***")
            #runs the query in athena
            bucket = configurations['bucket'] 
            path = s3_path + 'final_df.parquet'
            parquet_buffer = BytesIO()
            final_df.to_parquet(parquet_buffer)
            s3_resource = boto3.resource('s3')
            #stores the df into the desired s3 path
            s3_resource.Object(bucket,path).put(Body=parquet_buffer.getvalue())

            print("*** The query successfully completed for True Visits (Canada) for ", current_date_running)
            current_date_running = current_date_running - timedelta(1)

        #msck repair
        loading_partitions_table(table_name,path_data, database)
        print("\t** Process Completed for True Visits (Canada) **")
        
    else:
        print("*** The table for True visits is upto date *** ")





def visits_comparison(current_date_running,path_data,database,table_name):
    
    #calculate the number of days the function should run
    max_date_destination = wr.athena.read_sql_query(sql = '''Select cast(max(visit_date) as date) as max_date from aditya.stg_vip_visits_comparison''', database = database, workgroup = workgroup)
    max_date_destination = max_date_destination['max_date'][0]
    max_date_source = wr.athena.read_sql_query(sql = '''Select cast(max(visit_date) as date) as max_date from store_visitation_intermediate_orc.store_visitation_intermediate_orc_prod_c_cmb''', database = database, workgroup = workgroup)
    max_date_source = max_date_source['max_date'][0]
    current_date_running = copy.deepcopy(max_date_source)
    num_days = (max_date_source - max_date_destination).days
    
    print("*** Max Date updated for Visits Comparison", max_date_destination, " ***")

    
    print("*** No of days to backfill is", num_days, "days ***")
    db_base = 'store_visitation_intermediate_orc'
    s3_path_base = 's3://enigma-dw2/facts/store_visitation_intermediate_orc_staging_pmo250/c=cmb/'
    query_base = """MSCK REPAIR TABLE {}""".format(offline)
    running_query(query_base,db_base,s3_path_base)
    remove_file(path_data)
    command3 = 'aws s3 rm '+path_data+' --recursive --exclude "*" --include "*.csv"'
    
    if num_days > 0:
        for i in range(num_days):
            #splits year month and date from the given date to partition the s3 path
            year,month,day = calculate_ymd(current_date_running)
            s3_output = path_data + 'y={}/m={}/d={}/'.format(year,month,day)
            s3_path = 'analytics/vip/stg/vip_visits_comparison/' + 'y={}/m={}/d={}/'.format(year,month,day)
            prefix_s3_temp = s3_output.split('//')[1]
            prefix_s3 = prefix_s3_temp.replace("bucket",'/').split('//')[1]
            print("*** The script is running for", current_date_running, " ***")
            query_visits = f'''Unload(
                    WITH 
                    base_online AS 
                    (SELECT visit_date,
                    campaign_id,
                    adgroup_id,
                    SUM(visit) AS online_visits,
                    SUM(visit) FILTER (WHERE is_data_partner = 0) as online_visit_ap,
                    SUM(visit) FILTER (WHERE is_data_partner = 1) as online_visit_dp
                        FROM {online} base_online
                        LEFT JOIN  dimensions.publisher_dimension d ON CAST(d.publisher_id AS VARCHAR) = base_online.publisher_id
                        WHERE visit_date = '{current_date_running}'
                        AND exposure_type = 'IMPRESSION'
                        AND lag_bucket_id NOT IN ('-1', '7', '8')
                        GROUP BY 1, 2, 3),
                    base_offline AS
                    (SELECT visit_date,
                    adgroup_id,
                    campaign_id,
                    SUM(visit) AS offline_visits,
                    SUM(visit)  FILTER (WHERE is_data_partner = 0) as offline_visit_ap,
                    SUM(visit)  FILTER (WHERE is_data_partner = 1) as offline_visit_dp
                        FROM {offline} base_offline
                        LEFT JOIN  dimensions.publisher_dimension d ON CAST(d.publisher_id AS VARCHAR) = base_offline.publisher_id
                        WHERE visit_date = '{current_date_running}'
                        AND lag_bucket_id NOT IN ('-1', '7', '8')
                        AND exposure_type = 'AD_IMPRESSION'
                        GROUP BY 1, 2, 3)

                    SELECT 
                    a.visit_date,
                    tenant_id,
                    tenant_name,
                    account_id,
                    account_name, 
                    a.campaign_id,
                    a.adgroup_id,
                    CASE
                    WHEN business_line = 0 THEN 'Managed'
                    WHEN business_line = 1 AND business_type IN (1, 2) THEN 'Reseller'
                    WHEN business_line = 1 AND business_type IN (3, 5, 6) THEN 'Platform'
                    WHEN business_line = 1 AND business_type = 4 THEN 'Holding Co'
                    ELSE '' END AS business_line,
                    sum(offline_visits) as offline_visits,
                    sum(offline_visit_ap) as offline_visit_ap,
                    sum(offline_visit_dp) as offline_visit_dp,
                    sum(online_visits) as online_visits,
                    sum(online_visit_ap) as online_visit_ap,
                    sum(online_visit_dp) as online_visit_dp
                    FROM base_offline a
                    LEFT JOIN base_online b ON a.campaign_id = b.campaign_id AND a.adgroup_id = b.adgroup_id AND a.visit_date = b.visit_date 
                    INNER JOIN dimensions.campaign_dimension c ON a.campaign_id = c.campaign_id

                    where a.visit_date = '{current_date_running}'
                    group by 1,2,3,4,5,6,7,8)
                    TO '{s3_output}'
                    WITH (format = 'PARQUET',compression = 'SNAPPY')'''
            print('*** The query is running for Visits Comparison ***')
            print(query_visits)
            #delete_if_data_exists(s3_output)
            delete_all_objects_from_s3_folder(prefix_s3,0)                                   
            print("*** The path that the data is stored ", s3_output, "***")
            #runs the query in athena
            running_query(query_visits,database,s3_output)
            print("*** The query successfully completed for Visits comparison for ", current_date_running)
            current_date_running = current_date_running - timedelta(1)

        loading_partitions_table(table_name,path_data, database)
        
        print("\t** Process Completed for Visits Comparison **")
        
    else:
        print("*** The table for Visits Comparison is upto date *** ")
    
    command3 = 'aws s3 rm '+path_data+' --recursive --exclude "*" --include "*.csv"'
    remove_file(path_data)

    os.system(command3)


# In[15]:


def status_log(current_date_running,path_data,database,table_name):
    # Create a sample DataFrame
    data = {'Column1': [1]}
    latest_refresh_df = pd.DataFrame(data)
    # Get today's date
    today_date = current_date_running.date()
    # Add today's date as a new column to the DataFrame
    latest_refresh_df['date_latest_refresh'] = today_date
    
    #splits year month and date from the given date to partition the s3 path
    current_date_running = copy.deepcopy(current_date_running)
    year,month,day = calculate_ymd(current_date_running)
    s3_path = path_data + 'y={}/m={}/d={}/'.format(year,month,day)
    s3_output = 'analytics/vip/stg/status_log/' + 'y={}/m={}/d={}/'.format(year,month,day)
    
    #Latest date of Extract
    latest_date_extract = pd.read_sql(f"""SELECT max(date) as date_latest_extract FROM
                (SELECT date, COUNT(*) AS total_hours FROM (
                SELECT year, month, day, hour, 
                CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) AS date,
                COUNT(*) AS status2
                FROM enigma_etl_2017.status_log
                WHERE `key` IN ('/enigma/extract/us_display_dr','/enigma/extract/us_exchange')
                AND hour IS NOT NULL
                AND CAST(concat(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) > (CURDATE()-INTERVAL 31 DAY )
                GROUP BY 1, 2, 3, 4, 5) as a
                WHERE status2 >= 2
                group by 1) as b
                where total_hours=24""", connection_cent)
    
    #latest date of smart location
    latest_date_SL = pd.read_sql(f""" SELECT max(date) as date_latest_SL
                        FROM (SELECT CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) AS date
        
                                    , count(*) as total_hours
                            FROM enigma_etl_2017.status_log
                            WHERE `key` = 'sl_centroid/us/hourly'
                            AND hour IS NOT NULL
                            AND CAST(timestamp  as date) > (CURDATE()-INTERVAL 31 DAY )
                            GROUP BY  1
                            HAVING count(*) = 24
                            ) as base  """,connection_xad)
    
    # latest date of Science core 
    dt_scicore = pd.read_sql(f'''SELECT max(date) as date_latest_sciencecore FROM
                    (SELECT date, COUNT(*) AS total_hours FROM (
                    SELECT year, month, day, hour, CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) AS date,COUNT(*) AS status2
                    FROM enigma_etl_2017.status_log
                    WHERE `key` IN ('science_core_orc/us/display_dr','science_core_orc/us/sdk_dr','science_core_orc/us/display','science_core_orc/us/exchange')
                    AND hour IS NOT NULL
                    AND CAST(concat(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) > (CURDATE()-INTERVAL 31 DAY )
                    GROUP BY 1, 2, 3, 4, 5) as a
                    WHERE status2 >= 2
                    group by 1) as b
                    where total_hours=24''',connection_xad)
    
    #latest date of Sightings
    dt_sighting = pd.read_sql(f'''SELECT max(date) as date_latest_sighting FROM
                    (SELECT date, COUNT(*) AS total_hours FROM (
                    SELECT year, month, day, hour, CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) AS date,COUNT(*) AS status2
                    FROM enigma_etl_2017.status_log
                    WHERE `key` IN ('/staging/insights-etl/vhs-orc/us/exchange', '/staging/insights-etl/vhs-orc/us/display_dr')
                    AND hour IS NOT NULL
                    AND CAST(concat(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) > (CURDATE()-INTERVAL 31 DAY )
                    GROUP BY 1, 2, 3, 4, 5) as a
                    WHERE status2 >= 2
                    group by 1) as b
                    where total_hours=24''',connection_xad)
    
    #latest date of True visits (US)
    dt_true_visit = pd.read_sql(f""" SELECT max(date) as date_latest_truevisit
                        FROM (SELECT CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) AS date
                                    , count(*) as total_hours
                            FROM enigma_etl_2017.status_log
                            WHERE `key` = '/visit_store/true_visit_etl/us'
                            AND hour IS NOT NULL
                            AND CAST(timestamp  as date) > (CURDATE()-INTERVAL 31 DAY )
                            GROUP BY  1
                            HAVING count(*) = 24
                            ) as base  """ ,connection_cent)
    
    #latest date of Offline table
    latest_date_offline =pd.read_sql(f"""SELECT CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) AS date_latest_offline 
                                    FROM enigma_etl_2017.status_log
                                    where `key`= 'offline-attribution'
                                    order by timestamp desc 
                                    LIMIT 1""",connection_cent)
    
    #latest date for VIP Final ORC
    latest_date_VIP_ORC = wr.athena.read_sql_query(sql = '''select cast(max(visit_date) as date) as date_latest_VIP_ORC from store_visitation_intermediate_orc.store_visitation_intermediate_orc_prod_c_cmb''', 
                                                   database = database, workgroup = workgroup)
    
    #latest date of Online table
    latest_date_online =pd.read_sql(f"""SELECT CAST(CONCAT(CAST(year AS CHAR(50)),'-',CAST(month AS CHAR(50)),'-',CAST(day AS CHAR(50))) AS DATE) AS date_latest_online 
                                    FROM enigma_etl_2017.status_log
                                    where `key`= '/enigma/user_store/store_visitation_performance_summary/generate/cmb'
                                    order by timestamp desc 
                                    LIMIT 1""",connection_cent)
    
    #Concat all the dates in a single dataframe
    print('*** The query is running for Status Log ***')
    df_daily_date = pd.concat([latest_refresh_df['date_latest_refresh'],latest_date_extract,latest_date_SL,dt_scicore,dt_sighting,dt_true_visit,latest_date_offline,latest_date_VIP_ORC,latest_date_online], axis=1)
    #df_daily_date['date_latest_vip_orc'] = df_daily_date['date_latest_vip_orc'].astype(object)
    print(df_daily_date)
    
    print("*** The path that the data is stored ", s3_path, "***")
    bucket = bucket 
    path = s3_output + 'df_daily_date.parquet'
    parquet_buffer = BytesIO()
    df_daily_date.to_parquet(parquet_buffer)
    s3_resource = boto3.resource('s3')
    #stores the df into the desired s3 path
    s3_resource.Object(bucket,path).put(Body=parquet_buffer.getvalue())
    print("*** The query successfully completed for Status Log for ", current_date_running)


# In[16]:


lastUpdatedDate_stg = lastUpdated_advertiser_summary()
lastUpdatedDate_truevisits_us = lastUpdated_truevisits_us()
lastUpdatedDate_truevisits_ca = lastUpdated_truevisits_ca()
lastUpdatedDate_ORC = lastUpdated_storevisitationorc()
lastUpdatedDate_Offline = lastUpdated_offlineattribution()
current_date_running = datetime.today()


# In[17]:


last_minupdated_date = min(lastUpdatedDate_ORC,lastUpdatedDate_Offline)


# In[18]:


last_minupdated_date


# In[19]:


def main():
    #running the function to update the campaign monitor
    database_common ='aditya'
    path_campaign_monitor ='s3://bucket/analytics/vip/stg/campaign_monitor/'
    table_campaign_monitor = 'aditya.stg_vip_campaign_monitor' 
    campaign_monitor(lastUpdatedDate_stg,path_campaign_monitor,database_common,table_campaign_monitor)
    
    #running the function to update the status_log
    database_common ='aditya'
    path_statuslog ='s3://bucket/analytics/vip/stg/status_log/'
    table_statuslog = 'aditya.stg_vip_operational_status' 
    status_log(current_date_running,path_statuslog,database_common,table_statuslog)
    
    #running the function to update visits comparison 
    database_common ='aditya'
    path_visits_comparison ='s3://bucket/analytics/vip/stg/vip_visits_comparison/'
    table_visits_comparison = 'aditya.stg_vip_visits_comparison' 
    visits_comparison(last_minupdated_date,path_visits_comparison,database_common,table_visits_comparison)

    #running the function to update true visits – Both Canada and US

    # (US)
    database_common ='aditya'
    path_truevisits_us ='s3://bucket/analytics/vip/stg/sightings_truevisits/'
    table_truevisits_us = 'aditya.stg_vip_truevisits' 
    truevisits_us(lastUpdatedDate_truevisits_us,path_truevisits_us,database_common,table_truevisits_us)

   # (CA)
    database_common ='aditya'
    path_truevisits_ca ='s3://bucket/analytics/vip/stg/true_visit_canada/'
    table_truevisits_ca = 'aditya.stg_vip_truevisits_canada' 
    truevisits_ca(lastUpdatedDate_truevisits_ca,path_truevisits_ca,database_common,table_truevisits_ca)


    


# In[20]:


main()


