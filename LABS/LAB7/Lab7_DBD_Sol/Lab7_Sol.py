import sys
from pyspark import SparkConf, SparkContext
from datetime import datetime

conf = SparkConf().setAppName("Sol_Lab6")
sc = SparkContext(conf = conf)

#################################
# Task 1
#################################
# Set input and output folders
inputPath  = "/data/students/bigdata-01QYD/Lab7_DBD/register.csv" # args[0]
inputPath2 = "/data/students/bigdata-01QYD/Lab7_DBD/stations.csv" # args[1]
threshold  = 0.6 # args[2]
outputPath = "out_Lab7" # args[3]

# Read the content of the input file register.csv
inputRDD = sc.textFile(inputPath)


# Function that is used to remove the header and the lines with #free slots=0 && #used slots=0
def cleanData(line):
    # Remove header
    if line.startswith('s'):
        return False
    else:
        fields = line.split("\t")
        usedSlots = int(fields[2])
        freeSlots = int(fields[3])
        # Select the lines with freeSlots!=0 || usedSlots!=0
        if freeSlots != 0 or usedSlots != 0:
            return True
        else:
            return False
        
        
# Remove the header and the lines with #free slots=0 && #used slots=0
filteredRDD = inputRDD.filter(cleanData)


# Function that is used to map each input line to a pair
# key = (StationId,Weekday,Hour)
# value = (1,1) if the station is full, (1,0) if the station is not full
def checkFull(line):
    # station\ttimestamp\tused\tfree
    # 1\t2008-05-15 12:01:00\t0\t18
    fields = line.split("\t")
    stationId = fields[0]
    freeSlots = int(fields[3])
    timestamp = fields[1]
    
    datetimeObject = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")    
    dayOfTheWeek = datetimeObject.strftime("%A")
    hour = datetimeObject.hour

    if freeSlots == 0:
        # The station is full
        countTotReadingsTotFull = (1, 1)
    else:
        countTotReadingsTotFull = (1, 0)
        
    return ((stationId, dayOfTheWeek, hour), countTotReadingsTotFull)



# Map each line to a pair
# key = StationId_DayOfTheWeek_Hour
# value = (1,1) if the station is full, (1,0) if the station is not full
stationWeekDayHour = filteredRDD.map(checkFull)


# Count the total number of readings and "full" readings for each key
stationWeekDayHourCounts = stationWeekDayHour.reduceByKey(lambda p1, p2: (p1[0]+p2[0], p1[1]+p2[1]))


# Compute criticality for each key
stationWeekDayHourCriticality = stationWeekDayHourCounts.mapValues(lambda value: value[1]/value[0])


# Select only the pairs with criticality > threshold
selectedPairs = stationWeekDayHourCriticality.filter(lambda pair: pair[1]>= threshold)


# The next part of the code selects for each station the timeslot
# (dayOfTheWeek, Hour) with the maximum cardinality. 
# If there is more than one timeslot with the same criticality
# for the same station, only one of them is selected (see the problem specification)

# Create a new PairRDD with
# key = station Id
# value = DayOfTheWeek, Hour, Criticality
stationTimeslotCrit = selectedPairs.map(lambda StationWeekdayHourCrit:\
                                        (StationWeekdayHourCrit[0][0],\
                                         (StationWeekdayHourCrit[0][1], StationWeekdayHourCrit[0][2],\
                                          StationWeekdayHourCrit[1])\
                                        )\
                                       )

# Function to compare criticality between two timeslots
def compareCriticality(timeslotCrit1, timeslotCrit2):

    weekday1 = timeslotCrit1[0]
    weekday2 = timeslotCrit2[0]
    
    hour1 = timeslotCrit1[1]
    hour2 = timeslotCrit2[1]

    crit1 = timeslotCrit1[2]
    crit2 = timeslotCrit2[2]
    
    
    
    if crit1>crit2 or \
    (crit1==crit2 and hour1<hour2) or \
    (crit1==crit2 and hour1==hour2 and weekday1<weekday2):
        return timeslotCrit1
    else:
        return timeslotCrit2



# Select the timeslot (dayOfTheWeek, Hour) with the maximum criticality for each station
resultRDD = stationTimeslotCrit.reduceByKey(compareCriticality)


# Return
# - key = stationId
# - value = (long, lat) 
def extractStationLongLat(line):
    fields = line.split("\t")
    
    return (fields[0], (fields[1] ,fields[2]) )


# Read the location of the stations
stationLocation = sc.textFile(inputPath2).map(extractStationLongLat)


# Join the locations with the "critical" stations
resultLocations = resultRDD.join(stationLocation)


# Return a string that represents a KML marker
def formatKMLMarker(pair):
    # input
    # (stationId, ( (weekday, hour, criticality), (long, lat) ) )
    stationId = pair[0]
    
    weekday = pair[1][0][0]
    hour = pair[1][0][1]
    criticality = pair[1][0][2]
    coordinates = pair[1][1][0]+","+pair[1][1][1]
    
    result = "<Placemark><name>" + stationId + "</name>" + "<ExtendedData>"\
    + "<Data name=\"DayWeek\"><value>" + weekday + "</value></Data>"\
    + "<Data name=\"Hour\"><value>" + str(hour) + "</value></Data>"\
    + "<Data name=\"Criticality\"><value>" + str(criticality) + "</value></Data>"\
    + "</ExtendedData>" + "<Point>" + "<coordinates>" + coordinates + "</coordinates>"\
    + "</Point>" + "</Placemark>"
    
    return result


# Create a string containing the description of a marker, in the KML format, for each
# sensor and the associated information
resultKML = resultLocations.map(formatKMLMarker)


# Set the number of partitions to 1 for resultKML and store it in the output folder
resultKML.coalesce(1).saveAsTextFile(outputPath)

sc.stop()


