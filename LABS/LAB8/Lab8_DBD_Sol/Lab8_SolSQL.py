import sys
from pyspark import *
from pyspark.sql import SparkSession
from datetime import datetime

#################################
# Task 1
#################################

# Set input and output folders
inputPath  = "/data/students/bigdata-01QYD/Lab7_DBD/register.csv" # args[0]
inputPath2 = "/data/students/bigdata-01QYD/Lab7_DBD/stations.csv" # args[1]
threshold  = 0.6 # args[2]
outputPath = "out_Lab8SQL" # args[3]

# For the standalone version
spark = SparkSession.builder.appName("Spark Lab #7 - Template").getOrCreate()


# Read the content of the input file register.csv and store it into a DataFrame
# The input file has an header
# Schema of the input data:
# |-- station: integer (nullable = true)
# |-- timestamp: timestamp (nullable = true)
# |-- used_slots: integer (nullable = true)
# |-- free_slots: integer (nullable = true)
inputDF = spark.read.format("csv").option("delimiter", "\\t").option("header", True).option("inferSchema", True).load(inputPath)


# Assign the “table name” readings to inputDF
inputDF.createOrReplaceTempView("readings")

# Define a User Defined Function called full(Integer free_slots)
# that returns 
# -- 1 if free_slots is equal to 0
# -- 0 if free_slots is greater than 0
def fullFunction(free_slots):
    if free_slots==0:
        return 1
    else:
        return 0

# Define the UDF
# name: full
# output: integer value
spark.udf.register("full", fullFunction)

# Select only the lines with free_slots<>0 or used_slots<>0 and then 
# compute the criticality for each group (station, dayofweek, hour) (i.e., for each pair (station, timeslot))
# and finally select only the groups with criticality>threshold.
#
# The criticality of each group is equal to the average of full(free_slots)
# The schema of the returned dataset is:
# |-- station: integer (nullable = true)
# |-- dayofweek: string (nullable = true)
# |-- hour: integer (nullable = true)
# |-- criticality: double (nullable = true)
selectedPairsDF = spark.sql("""SELECT station, date_format(timestamp,'EE') as dayofweek, 
hour(timestamp) as hour, avg(full(free_slots)) as criticality 
FROM readings 
WHERE free_slots<>0 OR used_slots<>0
GROUP BY station, date_format(timestamp,'EE'), hour(timestamp)
HAVING avg(full(free_slots))>"""+str(threshold))


# Assign the “table name” criticals to selectedPairsDF
selectedPairsDF.createOrReplaceTempView("criticals")

# Read the content of the input file stations.csv and store it into a DataFrame
# The input file has an header
# Schema of the input data:
# |-- id: integer (nullable = true)
# |-- longitude: double (nullable = true)
# |-- latitude: double (nullable = true)
# |-- name: string (nullable = true)
stationsDF = spark.read.format("csv").option("delimiter", "\\t").option("header", True).option("inferSchema", True).load(inputPath2)


# Assign the “table name” stations to stationsDF
stationsDF.createOrReplaceTempView("stations")


# Join the selected critical "situations" with the stations table to
# retrieve the coordinates of the stations.
# Select only the column station, longitude, latitude and criticality
# and sort records by criticality (desc), station (asc)
selectedPairsIdCoordinatesCriticalityDF = spark.sql("""SELECT station, dayofweek, hour, 
longitude, latitude, criticality
FROM criticals, stations
WHERE criticals.station = stations.id
ORDER BY criticality DESC, station, dayofweek, hour""")

selectedPairsIdCoordinatesCriticalityDF.write.format("csv").option("header", True).save(outputPath)


# Close the Spark session
spark.stop()



