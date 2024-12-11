import sys
from pyspark import *
from pyspark.sql import SparkSession
from datetime import datetime

# Set input and output folders
inputPath  = "/data/students/bigdata-01QYD/Lab7_DBD/register.csv" # args[0]
inputPath2 = "/data/students/bigdata-01QYD/Lab7_DBD/stations.csv" # args[1]
threshold  = 0.6 # args[2]
outputPath = "out_Lab8DF" # args[3]


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


# Remove the lines with num free slots=0 && num used slots=0
filteredDF = inputDF.filter("free_slots<>0 OR used_slots<>0")


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


# Define a DataFrame with the following schema:
# |-- station: integer (nullable = true)
# |-- dayofweek: string (nullable = true)
# |-- hour: integer (nullable = true)
# |-- fullstatus: integer (nullable = true) - 1 = full, 0 = non-full
stationWeekdayHourDF = filteredDF.selectExpr("station", "date_format(timestamp,'EE') as dayofweek", "hour(timestamp) as hour", "full(free_slots) as fullstatus")


# Define one group for each combination "station, dayofweek, hour"
rgdStationWeekdayHourDF = stationWeekdayHourDF.groupBy("station", "dayofweek", "hour")


# Compute the criticality for each group (station, dayofweek, hour),
# i.e., for each pair (station, timeslot)
# The criticality is equal to the average of fullStatus
stationWeekdayHourCriticalityDF = rgdStationWeekdayHourDF.agg({"fullStatus": "avg"}).withColumnRenamed("avg(fullStatus)", "criticality")

# Select only the lines with criticality > threshold
selectedPairsDF = stationWeekdayHourCriticalityDF.filter("criticality>"+str(threshold))

# Read the content of the input file stations.csv and store it into a DataFrame
# The input file has an header
# Schema of the input data:
# |-- id: integer (nullable = true)
# |-- longitude: double (nullable = true)
# |-- latitude: double (nullable = true)
# |-- name: string (nullable = true)
stationsDF = spark.read.format("csv").option("delimiter", "\\t").option("header", True).option("inferSchema", True).load(inputPath2)

# Join the selected critical "situations" with the stations table to
# retrieve the coordinates of the stations
selectedPairsCoordinatesDF = selectedPairsDF.join(stationsDF, selectedPairsDF.station == stationsDF.id)

# Sort the content of the DataFrame
selectedPairsIdCoordinatesCriticalityDF = selectedPairsCoordinatesDF.selectExpr("station", "dayofweek", "hour", "longitude", "latitude", "criticality").sort(selectedPairsCoordinatesDF.criticality.desc(),      selectedPairsCoordinatesDF.station,      selectedPairsCoordinatesDF.dayofweek,      selectedPairsCoordinatesDF.hour)

selectedPairsIdCoordinatesCriticalityDF.write.format("csv").option("header", True).save(outputPath)

# Close the Spark session
spark.stop()

