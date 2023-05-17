import pyodbc
import pandas as pd
import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/neera/OneDrive/Desktop/etl-demo-project-385707-17dcdf5a87ca.json"

conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=DESKTOP-OKQTFSU\SQLEXPRESS;'
                            'Database=covid_database;'
                            'Trusted_Connection=yes;')

query = "select * from table1"
df = pd.read_sql(query,conn)
df = df.dropna(thresh=6)
mean = df.mean()
df = df.fillna(mean)
client = bigquery.Client()
df.to_gbq(
      
    project_id = 'etl-demo-project-385707',
    destination_table = '01etl.table01',
    if_exists = 'append',
    chunksize = 10000
)

