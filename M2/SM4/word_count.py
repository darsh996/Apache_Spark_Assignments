from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
print(sc.textFile("file:///home/talentum/test-jupyter/P2/M2/SM4/4_AdvancedRddActions/constitution.txt")\
.flatMap(lambda x: x.split(" "))\
.map(lambda w: (w, 1))\
.reduceByKey(lambda x, y: x + y)\
.map(lambda t : (t[1],t[0]))\
.sortByKey(ascending = False)\
.take(5))
