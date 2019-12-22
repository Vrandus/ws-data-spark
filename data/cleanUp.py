import pyspark
from pyspark import SparkConf
conf = SparkConf().setAppName('Clean up csv')
sc = pyspark.SparkContext(conf=conf)
spark = pyspark.sql.SparkSession(sc)
df = spark.read.csv('DataSample.csv', header=True, inferSchema=True)
print("BEFORE CLEAN UP >>>> ", df.count())


# cleaning up
# theres an extra space before TimeSt 
from pyspark.sql.functions import col
# removing extra space by creating a new dataframe with alias on col(' TimeSt')
df = df.select(col('_ID'), col(' TimeSt').alias('TimeSt'), col('Country'), col('Province'), col('City'), col('Latitude'), col('Longitude'))
filtered = df.dropDuplicates(['TimeSt', 'Latitude', 'Longitude']) 
df
# Noticed there were duplicates in POI that should probably be removed when i got to Problem 2

# POIList.csv cleanup

poiList = spark.read.csv('POIList.csv', header=True, inferSchema=True)
poiList = poiList.select(col('POIID'), col(' Latitude').alias('Latitude'), col('Longitude'))

poiList = poiList.dropDuplicates(['Latitude', 'Longitude'])


print("AFTER CLEAN UP >>>> ", filtered.count())
poiList.show()
poiList.write.mode('overwrite').option('header', 'true').save("POIList-filtered.csv", format='csv')
filtered.write.mode('overwrite').option('header', 'true').save("DataSample-filtered.csv", format='csv')