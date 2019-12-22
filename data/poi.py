import pyspark
from pyspark import SparkConf

conf = SparkConf().setAppName('Closest Point of Interest')
sc = pyspark.SparkContext(conf=conf)
spark = pyspark.sql.SparkSession(sc)

# read csv files to memory
df = spark.read.load('/tmp/data/DataSample-filtered.csv', header=True, format='csv', inferSchema=True)
print("ROW COUNT >>>> ", df.count())
poiList = spark.read.load('/tmp/data/POIList-filtered.csv', header=True, format='csv', inferSchema=True)

# from GFG https://www.geeksforgeeks.org/program-distance-two-points-earth/
from math import radians, cos, sin, asin, sqrt 
def distance(lat1, lat2, lon1, lon2): 
      
    # The math module contains a function named 
    # radians which converts from degrees to radians. 
    lon1 = radians(lon1) 
    lon2 = radians(lon2) 
    lat1 = radians(lat1) 
    lat2 = radians(lat2) 
       
    # Haversine formula  
    dlon = lon2 - lon1  
    dlat = lat2 - lat1 
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
  
    c = 2 * asin(sqrt(a))  
     
    # Radius of earth in kilometers. Use 3956 for miles 
    r = 6371
       
    # calculate the result 
    return(c * r) 


# finding closest point of interest for each entity in DataSample

dataSample = df.collect()       # probably some way to parallelize these lists
listPoi = poiList.collect()
import math
closestList = []
for req in dataSample:
    closest = 0, "poi", 999999      # instantiating tuple with some large int for distance
    for poi in listPoi:
        # library might already exist to do this? 
        # later realized when i got to problem 3 that im assuming latitude and longitude are on a flat plane, not a spherical earth
        # googled a bit and found the haversine formula implementation for python
        # distance = req._ID, poi.POIID, math.sqrt((poi.Latitude - req.Latitude) ** 2 + (poi.Longitude - req.Longitude) ** 2)
        
        # replace earlier implementation of distance with haversine formula
        dist = req._ID, poi.POIID, distance(poi.Latitude, req.Latitude, poi.Longitude, req.Longitude)
        
        if dist[2] < closest[2]:
            closest = dist
    closestList.append(closest) #add to list

#converting list to dataframe then using join on the key _ID
closestDf = spark.createDataFrame(closestList, ['_ID', 'closestPOI', 'distance']) 
df = df.join(closestDf, df._ID == closestDf._ID).select(df._ID, \
                                                df.TimeSt, df.Country, df.Province, df.City, df.Latitude,    
                                                df.Longitude, closestDf.closestPOI, closestDf.distance)
df.show(10)
# writing to another file for the next job 
df.write.mode('overwrite').option('header', 'true').save("DataSample-with-ClosestPOI.csv", format='csv')
