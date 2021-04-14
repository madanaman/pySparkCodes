#!/usr/bin/env python
# coding: utf-8



from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
from graphframes import *

sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

delays_fp = "/Volumes/Data\ Disk/data_disk_workspace/pythonProjects/pySparkRecipes/data/departuredelays.csv"
apts_fp = "/Volumes/Data\ Disk/data_disk_workspace/pythonProjects/pySparkRecipes/data/airport-codes-na.txt"

apts = spark.read.csv(apts_fp, inferSchema=True, header=True, sep='\t')
apts.createOrReplaceTempView("apts")

# In[29]:


apts.show(5)

# In[16]:


deptsDelays = spark.read.csv(delays_fp, inferSchema=True, header=True, sep=',')
deptsDelays.createOrReplaceTempView("deptsDelays")
deptsDelays.cache()

# In[30]:


# Available IATA codes from the departuredelays sample dataset
iata = spark.sql("""
    select distinct iata 
    from (
        select distinct origin as iata 
        from deptsDelays 

        union all 
        select distinct destination as iata 
        from deptsDelays
    ) as a
""")
iata.createOrReplaceTempView("iata")
iata.show(5)

# In[31]:


# Only include airports with atleast one trip from the departureDelays dataset
airports = sqlContext.sql("""
    select f.IATA
        , f.City
        , f.State
        , f.Country 
    from apts as f 
    join iata as t 
        on t.IATA = f.IATA
""")
airports.registerTempTable("airports")
airports.cache()
airports.show(5)


# In[38]:


@f.udf
def toDate(ipDate):
    ipDate = str(ipDate)
    year = '2021-'
    month = "1" + ipDate[0:1] + '-'
    day = ipDate[1:3] + ' '
    hour = ipDate[3:5] + ':'
    minute = ipDate[5:7] + ':00'
    return year + month + day + hour + minute


# In[39]:


deptsDelays = deptsDelays.withColumn('normalDate', toDate(deptsDelays.date))
deptsDelays.createOrReplaceTempView("deptsDelays")
deptsDelays.show(50)

# In[40]:


# Get key attributes of a flight
deptsDelays_GEO = spark.sql("""
    select cast(f.date as int) as tripid
        , cast(f.normalDate as timestamp) as localdate
        , cast(f.delay as int)
        , cast(f.distance as int)
        , f.origin as src
        , f.destination as dst
        , o.city as city_src
        , d.city as city_dst
        , o.state as state_src
        , d.state as state_dst 
    from deptsDelays as f 
    join airports as o 
        on o.iata = f.origin 
    join airports as d 
        on d.iata = f.destination
""")
# Create Temp View
deptsDelays_GEO.createOrReplaceTempView("deptsDelays_GEO")

# Cache and Count
deptsDelays_GEO.cache()
deptsDelays_GEO.show(5)

# Create Vertices (airports) and Edges (flights)
vertices = airports.withColumnRenamed("IATA", "id").distinct()
edges = deptsDelays_GEO.select("tripid", "delay", "src", "dst", "city_dst", "state_dst")

# Cache Vertices and Edges
edges.cache()
vertices.cache()

# This GraphFrame builds up on the vertices and edges based on our trips (flights)
graph = GraphFrame(vertices, edges)