import sys
from pyspark import SparkConf, SparkContext
from typing import Tuple

conf = SparkConf().setAppName("Name of my application")
sc = SparkContext(conf = conf)

# Set frefix, input and output folder (the values are specified by means of three input parameters)


inputPath  = sys.argv[1] 
outputPath = sys.argv[2]
outputPath2 = sys.argv[3]
prefix = sys.argv[4] 

# Read input data
wordsFrequenciesRDD = sc.textFile(inputPath)

# Task 1

# Keep only the lines containing words that start with the prefix “ho”
selectedLinesRDD = wordsFrequenciesRDD.filter(lambda line: line.startswith(prefix))

# Print on the standard output the following statistics
# - The number of selected lines
numLines = selectedLinesRDD.count()
print("Num. selected lines: "+ str(numLines) ) 

# Print on the standard output the following statistics
# - The maximum frequency (maxfreq) among the ones of the selected lines (i.e., the maximum value of freq in the lines obtained by applying the filter).

# Select the values of frequency
maxfreqRDD = selectedLinesRDD.map(lambda line: float(line.split("\t")[1]))

# Compute the maximu value
maxfreq = maxfreqRDD.reduce(lambda freq1, freq2: max(freq1, freq2) )

# Print maxfreq on the standard output
print("Maximum frequency: "+ str(maxfreq) ) 

# Task 2
# Your application must select those lines that contain words with a frequency (freq) 
# greater than 80% of the maximum frequency (maxfreq) computed before.

# Keep only the lines with a frequency freq greater than 0.8*maxfreq.
selectedLinesMaxFreqRDD = selectedLinesRDD.filter(lambda line: float(line.split("\t")[1])>0.8*maxfreq)

# Count the number of selected lines and print this number on the standard output
numLinesMaxfreq = selectedLinesMaxFreqRDD.count()
print("Num. selected lines with freq > 0.8*maxfreq: "+ str(numLinesMaxfreq) ) 

# Select only the words (first field)
selectedWordsRDD = selectedLinesMaxFreqRDD.map(lambda line: line.split("\t")[0])


# Task 3
# Your application must compute all the six groups and determine the number of words belonging to each group
# each group is defined based on word frequency
# group 0: [0, 99]
# group 1: [100, 199]
# group 2: [200, 299]
# group 3: [300, 399]
# group 4: [400, 499]
# group 5: [500, +inf)
def compute_group(line: str) -> Tuple[str, int]:
    fields = line.split('\t')
    freq = int(fields[1])
    if freq >= 500:
        group = 5
    else:
        group = freq // 100
    return (f'Group {group}', 1)

# compute the pair RDD with key = group, value = +1
groupPairRDD = wordsFrequenciesRDD.map(compute_group)
# use a reduceByKey to sum all the +1 in the value part
countPerGroupPairRDD = groupPairRDD.reduceByKey(lambda v1, v2: v1 + v2)


# Save output
# Save the selected words (without frequency) in an output folder (one word per line)
selectedWordsRDD.saveAsTextFile(outputPath)

# Save groups
countPerGroupPairRDD.saveAsTextFile(outputPath2)

sc.stop()

