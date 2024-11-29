import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Name of my application")
sc = SparkContext(conf = conf)

#################################
# Task 1
#################################
# Set input and output folders
inputPath  = "/data/students/bigdata-01QYD/Lab6_DBD/Reviews.csv" # "ReviewsSample.csv"
outputPath = "res_out_Lab6/" 

# Read the content of the input file
reviewsRDD = sc.textFile(inputPath)

# Discard the header
reviewsRDDnoHeader = reviewsRDD.filter(lambda line: line.startswith("Id,")==False)

# This Python function splits the input line and returns a tuple (userId, productId)
def extractUserIdProductID(line):
    columns = line.split(",")
    userId= columns[2]
    productId= columns[1]
    
    return (userId,productId)


# Generate one pair (UserId, ProductId) from each input line
pairUserProductRDD = reviewsRDDnoHeader.map(extractUserIdProductID)


# Remove duplicate pairs, if any
pairUserProductDistinctRDD = pairUserProductRDD.distinct()

# Generate one "transaction" for each user
# (user_id, list of the product_ids reviewed by user_id)
UserIDListOfReviewedProductsRDD = pairUserProductDistinctRDD.groupByKey()

# We are interested only in the value part (the lists of products that have been reviewed together)
transactionsRDD = UserIDListOfReviewedProductsRDD.values()

# Given an input transaction (i.e., a list of products reviewed by the same user), 
# this Python function returns all the possible pair of products. Each pair of product is associated with
# a frequency equal to 1. Hence, this method returns a set of (key, value) pairs, where
# - key = pairs of products
# - value = 1
def extractPairsOfProducts(transaction):

    products = list(transaction)

    returnedPairs = []
    
    for product1 in products:
        for product2 in products:
            if product1<product2:
                returnedPairs.append( ((product1, product2), 1) )
                
    return returnedPairs


# Generate an RDD of (key,value) pairs, where
# - key = pairs of products
# - value = 1

# One pair is returned for each combination of products appearing in the same transaction  
pairsOfProductsOneRDD = transactionsRDD.flatMap(extractPairsOfProducts)


# Count the frequency (i.e., number of occurrences) of each key (= pair of products)
pairsFrequenciesRDD = pairsOfProductsOneRDD.reduceByKey(lambda count1, count2: count1 + count2)

# Select only the pairs that appear more than once and their frequencies.
atLeast2PairsFrequenciesRDD = pairsFrequenciesRDD.filter(lambda inputTuple: inputTuple[1]> 1)

# Sort pairs of products by decreasing frequency
atLeast2PairsFrequenciesSortedRDD = atLeast2PairsFrequenciesRDD.sortBy(lambda inputTuple: inputTuple[1], False).cache()

# Store the result in the output folder
atLeast2PairsFrequenciesSortedRDD.saveAsTextFile(outputPath)


#################################
# Task 2 - Bonus track
#################################

# The pairs of products in atLeast2PairsFrequenciesSortedRDD are already sorted by frequency.
# The first 10 pairs are already the top 10 ones
topPairsOfProducts = atLeast2PairsFrequenciesSortedRDD.take(10)


# Print the selected pairs of products on the standard ouput of the driver
for pairOfProducts in topPairsOfProducts:
    print(pairOfProducts)

sc.stop()

