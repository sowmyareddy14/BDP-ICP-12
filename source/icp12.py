from pyspark import SparkContext,SQLContext
from graphframes import *
import pyspark.sql.functions as f

sc = SparkContext.getOrCreate()
sc.addPyFile('C:\spark-3.0.0-preview2-bin-hadoop2.7\jars\graphframes-0.8.0-spark3.0-s_2.12.jar')
sqlcontext=SQLContext(sc)

#question 1
station_data_df= sqlcontext.read.format("csv").option("header", "true").csv(r'C:\Users\jagad\Downloads\201508_station_data.csv')
trips_df= sqlcontext.read.format("csv").option("header", "true").csv(r'C:\Users\jagad\Downloads\201508_trip_data.csv')
station_data_df.show()

#question 2 - lit is used to interact with column literals in pyspark
concat = station_data_df.select(f.concat("lat",f.lit("=>"),"long"))
concat.show()

#question 3
station_data_df_dropduplicates =  station_data_df.dropDuplicates()
station_data_df_dropduplicates.show()

#question 4 and 5
station_data_df_renamecolumns=station_data_df.withColumnRenamed("station_id","id").select("id").distinct()
station_data_df_renamecolumns.show()

#question 6
station_vertices = station_data_df.withColumnRenamed("name","id").select("id").distinct()
station_vertices.show()

trip_edges=trips_df.withColumnRenamed("Start Station","src")\
.withColumnRenamed("End Station","dst")\
.select("src","dst")

g=GraphFrame(station_vertices,trip_edges)


#Question 7
g.vertices.show()

#question 8
g.edges.show()

#question 9
g.inDegrees.show()

#question 10
g.outDegrees.show()

#question 11
g.find("(a)-[b]->(e)").show() #search verices connected by edges


#Bonus
#-----1.Vertex degree---------
g.degrees.show()

#-----2.finding the common destinations-------
heighestdestination = g.edges.groupBy("src", "dst").count().orderBy(desc("count")).limit(10)
heighestdestination.show()

#----3.highest ratio of in degrees but fewest out degrees----
inDegrees = g.inDegrees
outDegrees = g.outDegrees
degreeRatio = inDegrees.join(outDegrees, inDegrees["id"] == outDegrees["id"]).drop(outDegrees["id"]).selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
degreeRatio.orderBy(desc("degreeRatio")).limit(10).show()

