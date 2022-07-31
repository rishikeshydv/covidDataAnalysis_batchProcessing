import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType
from sqlalchemy import create_engine 
import pandas as pd

session = SparkSession.builder.master("local").appName('hdfs_test').getOrCreate()
data_schema = StructType.add("Date","datetime").add("Country","string").add("Confirmed",'integer').add('Recovered','integer').add("Deaths",'integer')
df = session.read.csv("hdfs://localhost:9000//user/User/container_1", schema=data_schema)
df.show(10)

# view the schema
df.printSchema()

grouping = df.groupBy('Country')['Confirmed', 'Recovered', 'Deaths'].sum()

#writing the aggregated data into database tables
hostname = 'localhost'
database = 'fort_biz'
username = 'postgres'
pwd = 'rishiBLOOMED!321'
port_id = '5432'
engine = create_engine('postgresql+psycopg2://postgres:rishiBLOOMED!321@localhost:5432/fort_biz')

db_table = engine.execute("CREATE TABLE IF NOT EXISTS covid_table (Country, Total_Count);")
pd_df = pd.DataFrame(df, columns= ['Country','Total_Count'])
pd_df.to_sql('covid_table',engine, if_exists='replace', index = False)
display_data = engine.execute("SELECT * FROM covid_table;")










