import pyspark
from pyspark import SparkConf
from pyspark.sql import functions as F

conf = SparkConf().setAppName('Data Analysis')
sc = pyspark.SparkContext(conf=conf)
spark = pyspark.sql.SparkSession(sc)

# read csv files to df and poiList
df = spark.read.load('/tmp/data/DataSample-with-ClosestPOI.csv', header=True, format='csv', inferSchema=True)

poiList = spark.read.load('/tmp/data/POIList-filtered.csv', header=True, format='csv', inferSchema=True)


# an SQL Query can be used to quickly find average distance for each 
# SELECT closestPOI AS POI, avg(distance) FROM df GROUP BY closestPOI
# added restriction to gbPOI to remove outliers, 5000km sounds more reasonable
 
gbPOI = df.where(df.distance < 5000).groupBy(df.closestPOI.alias('POI')) # reducing calls to this function

stdAvg = gbPOI.agg(F.stddev(df.distance), F.mean(df.distance))
stdAvg.show()

# to find the radius of each POI, we can assume that POI circles have the radii of their farthest respective request
# after that, get density by area/requestNumber

radius = df.groupBy(df.closestPOI.alias('POI')).agg(F.max(df.distance), F.count(df.closestPOI))
# found outliers in the datasample which is why max(df.distance) returns large numbers
radius.show()

# more reasonable radius values
radius = gbPOI.agg(F.max(df.distance).alias('Radius'), F.count(df.closestPOI).alias('ReqNumber'))

radius = radius.withColumn('Density', (3.14 * radius.Radius ** 2) / radius.ReqNumber)
radius.show()