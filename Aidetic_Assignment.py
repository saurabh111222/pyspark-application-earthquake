import pandas as pd
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


df = pd.read_csv('database.csv')
df.head()

# generating a string of column name that will be used for creating the table in sql database.
columns_sql = []
columns = []
for col in df.columns:
    columns.append(f'{col.replace(" ", "_")}')
    columns_sql.append(f'{col.replace(" ", "_")}'+' string')
columns_str_sql = ','.join(columns_sql)

# modefying table name as it should match with the table schema.
df.columns = columns

try:
   
    # Connect to DB and create a cursor
    sqliteConnection = sqlite3.connect('sql.db')
    cursor = sqliteConnection.cursor()
    print('Database initiated')
 
    # Write a query and execute it with cursor
    query = f"""CREATE TABLE IF NOT EXISTS neic_earthquakes(Date string,Time string,Latitude string,Longitude string,Type string,Depth string,Depth_Error string,Depth_Seismic_Stations string,Magnitude string,Magnitude_Type string,Magnitude_Error string,Magnitude_Seismic_Stations string,Azimuthal_Gap string,Horizontal_Distance string,Horizontal_Error string,Root_Mean_Square string,ID string,Source string,Location_Source string,Magnitude_Source string,Status string)"""
    cursor.execute(query)
    print("Table has been created")
    
    df.to_sql('neic_earthquakes', sqliteConnection, if_exists='replace', index = False)
    print("Data has been loaded into the table")
    
    cursor.execute("SELECT * FROM neic_earthquakes LIMIT 10").fetchall()
    
except Exception as e:
    print(f"Error: {e}")
	
print("Tables: ", cursor.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name; ").fetchall())

#creating spark session using bulder method

spark = SparkSession. \
builder. \
config('spark.shuffle.useOldFetchProtocol', 'true'). \
config("spark.sql.warehouse.dir", "/user/itv009225/warehouse"). \
config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0'). \
enableHiveSupport(). \
master('yarn'). \
getOrCreate()


# As i am running this spark code on some third party spark cluster setup and facing error regarding the sql driver required. This would run in your cluster if 
# it has the driver installed. Because of this reason I have to create the dataframe on the file directly.

# df = spark.read.format('jdbc').\
#      options(url='jdbc:sqlite:Chinook_Sqlite.sqlite',\
#      dbtable='neic_earthquakes',driver='org.sqlite.JDBC').load()


df = spark.read.format("csv") \
     .option("inferSchema", "true") \
     .option("header","true") \
     .load("database.csv")

df = df.withColumn("date_new", to_date('Date', 'MM/dd/yyyy'))

# How does the Day of a Week affect the number of earthquakes?
print("****How does the Day of a Week affect the number of earthquakes?**** \n")
df.filter("Type == 'Earthquake' AND date_new IS NOT NULL") \
  .withColumn("dayofweek", dayofweek('date_new')) \
  .select("dayofweek", "date_new", "Type") \
  .groupBy("dayofweek") \
  .count().orderBy("dayofweek")


# What is the relation between Day of the month and Number of earthquakes that happened in a year?
print("****What is the relation between Day of the month and Number of earthquakes that happened in a year?***")
df.filter("Type == 'Earthquake' AND date_new IS NOT NULL") \
    .withColumn("dayofmonth", dayofmonth(col("date_new"))) \
    .withColumn("year", year(col("date_new"))) \
    .selectExpr("year","dayofmonth", "date_new") \
    .groupBy("dayofmonth", "year") \
    .count().orderBy("year", "dayofmonth")
    
    
# What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?
print("***** What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?****")
df.filter("Type == 'Earthquake' AND date_new IS NOT NULL AND year(date_new) >= 1965 AND year(date_new) <= 2016") \
    .withColumn("month", month(col("date_new"))) \
    .withColumn("year", year(col("date_new"))) \
    .groupBy("year", "month") \
    .count() \
    .groupBy("month") \
    .agg(round(avg("count")).alias("avg_freq")) \
    .orderBy("month")


# How has the earthquake magnitude on average been varied over the years?
print("***How has the earthquake magnitude on average been varied over the years?***")
df.filter("Type == 'Earthquake' AND date_new IS NOT NULL") \
    .withColumn("year", year(col("date_new"))) \
    .groupBy("year") \
    .agg(avg("Magnitude").alias("avg_magnitude")) \
    .orderBy("year")
    
    
 # How does year impact the standard deviation of the earthquakes?
 print("***How does year impact the standard deviation of the earthquakes?***")
df.filter("Type == 'Earthquake' AND date_new IS NOT NULL") \
    .withColumn("year", year(col("date_new"))) \
    .groupBy("year") \
    .agg(stddev("Magnitude").alias("stddev_magnitude")) \
    .orderBy("year")
    
 # Where do earthquakes occur very frequently?
 
print("***Where do earthquakes occur very frequently?***")
df.filter("Type == 'Earthquake' AND date_new IS NOT NULL") \
    .withColumn("year", year(col("date_new"))) \
    .groupBy("Location Source") \
    .count() \
    .orderBy("count", ascending=False)
    
