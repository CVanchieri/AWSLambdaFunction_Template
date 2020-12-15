### necessary imports ###
import os
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import requests
import datetime
from datetime import datetime
from pandas import DataFrame

ROOT = Path(__file__).resolve().parents[0]
start = datetime.now()

### lambda handler ###
def lambda_handler(event, context):
    print("---> connect to the Covid19 api")
    ### COVID-19 ###
    response = requests.get("http://covidtracking.com/api/us") # latest daily pull total
    # response = requests.get("https://covidtracking.com/api/us/daily") # all daily totals 

    print(f'--> status code: {response.status_code}')
    print("---> api connected")

    covid_cs = response.json()
    df = pd.json_normalize(covid_cs)
    df = df.drop(columns=['hash'])
    print(df.shape)
    print(df.head())

    print("--- authenticate AWS connection ---")
    AWSdatabase = os.getenv("AWSDATABASE")
    AWSuser = os.getenv("AWSUSER")
    AWSpassword = os.getenv("AWSPASSWORD")
    AWShost = os.getenv("AWSHOST")
    AWSport = os.getenv("AWSPORT")
    sql_AWS = os.getenv("AWSSQL")

    ## connect to AWS database ###
    connection = psycopg2.connect(database=AWSdatabase,
                                  user=AWSuser,
                                  password=AWSpassword,
                                  host=AWShost,
                                  port=AWSport)

    cur = connection.cursor()
    print("---> complete")

    ### push the final dataframe to the SQL database ###
    print("--- push to dataframe AWS database ---")
    engine = create_engine(sql_AWS) # create engine
    df.to_sql('covid19us', con=engine, index=False, if_exists='append') # push the dataframe to the database
    print("---> complete")

    print('--- timer ---')
    break2 = datetime.now()
    print("Elapsed time: {0}".format(break2-start)) # show timer 
