from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Log filtering").setMaster("local")
sc = SparkContext(conf=conf)
input = "data/Ex30/data/log.txt"
inputRDD = sc.textFile(input)
googleRDD = inputRDD.filter(lambda line: "google" in line)
googleRDD.collect()


