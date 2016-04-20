// Databricks notebook source exported at Wed, 20 Apr 2016 12:42:24 UTC
// MAGIC %md <img src='https://daks2k3a4ib2z.cloudfront.net/5649a7838b8168f16dcf153c/56efeb08803433e7393aceda_comSysto-logo_200px.png' style='width:400px'>

// COMMAND ----------

// MAGIC %md # Making sense of RDDs, DataFrames, SparkSQL and Datasets APIs
// MAGIC ### Motivation
// MAGIC - overview over the different Spark APIs for working with structured data.
// MAGIC ### Timeline of Spark APIs
// MAGIC - Spark **1.0** used the **RDD** API - *a Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, partitioned collection of elements that can be operated on in parallel.*
// MAGIC - Spark **1.3** introduced the **DataFrames** API - *a distributed collection of data organized into named columns. Also a well known concept from R / Python Pandas.*
// MAGIC - Spark **1.6** introduced an experimental **Datasets** API - *extension of the DataFrame API that provides a type-safe, object-oriented programming interface.*

// COMMAND ----------

// MAGIC %md # RDD
// MAGIC ## RDD - Resilient Distributed Dataset
// MAGIC ### **Functional** transformations on partitioned collections of **opaque** objects.

// COMMAND ----------

// MAGIC %md #### Define case class representing schema of our data.
// MAGIC Each field represent *column* of the *DB*.

// COMMAND ----------

case class Person(name: String, age: Int)

// COMMAND ----------

// MAGIC %md #### Create parallelized collection (RDD)

// COMMAND ----------

val peopleRDD = sc.parallelize(Array(
  Person("Lars", 37), 
  Person("Sven", 38), 
  Person("Florian", 39),
  Person("Dieter", 37)
))

// COMMAND ----------

// MAGIC %md RDD of **type Person**

// COMMAND ----------

val rdd = peopleRDD
  .filter(_.age > 37)

// COMMAND ----------

// MAGIC %md **NB: Return Person objects**

// COMMAND ----------

rdd
  .collect
  .foreach(println(_))

// COMMAND ----------

// MAGIC %md # DataFrames
// MAGIC ### **Declarative** transformations on partitioned collection of **tuples**.

// COMMAND ----------

val peopleDF = peopleRDD.toDF

// COMMAND ----------

peopleDF.show()

// COMMAND ----------

peopleDF.printSchema()

// COMMAND ----------

// MAGIC %md Show only **age** column

// COMMAND ----------

peopleDF.select("age").show()

// COMMAND ----------

// MAGIC %md **NB: Result set consists of Arrays of String und Ints**

// COMMAND ----------

peopleDF
  .filter("age > 37")
  .collect
  .foreach(row => println(row))

// COMMAND ----------

// MAGIC %md # DataSets

// COMMAND ----------

// MAGIC %md #### Create DataFrames from RDDs
// MAGIC ##### Implicit conversion is also available

// COMMAND ----------

val peopleDS = peopleRDD.toDS

// COMMAND ----------

// MAGIC %md **NB: Result set consist of Person objects**

// COMMAND ----------

val ds = peopleDS
  .filter(_.age > 37)

// COMMAND ----------

ds.collect
  .foreach(println(_))

// COMMAND ----------

ds.queryExecution.analyzed

// COMMAND ----------

ds.queryExecution.optimizedPlan

// COMMAND ----------

ds.collect
  .foreach(println(_))

// COMMAND ----------

// MAGIC %md #Spark SQL

// COMMAND ----------

// Get SQL context from Spark context
// NB: "In Databricks, developers should utilize the shared HiveContext instead of creating one using the constructor. In Scala and Python notebooks, the shared context can be accessed as sqlContext. When running a job, you can access the shared context by calling SQLContext.getOrCreate(SparkContext.getOrCreate())."

import org.apache.spark.sql.SQLContext
val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())

// COMMAND ----------

// MAGIC %md **Register DataFrame for usage via SQL**

// COMMAND ----------

peopleDF.registerTempTable("sparkPeopleTbl")

// COMMAND ----------

// MAGIC %md The results of SQL queries are DataFrames and support all the usual RDD operations.

// COMMAND ----------

sqlContext.sql("SELECT * FROM sparkPeopleTbl WHERE age > 37")

// COMMAND ----------

// MAGIC %md Print execution plan

// COMMAND ----------

sql("SELECT * FROM sparkPeopleTbl WHERE age > 37").queryExecution.analyzed

// COMMAND ----------

// MAGIC %md Optimized by built-in optimizer execution plan 

// COMMAND ----------

sql("SELECT * FROM sparkPeopleTbl WHERE age > 37").queryExecution.optimizedPlan

// COMMAND ----------

// MAGIC %sql SELECT * FROM sparkPeopleTbl WHERE age > 37

// COMMAND ----------

sql("SELECT * FROM sparkPeopleTbl WHERE age > 37").collect

// COMMAND ----------

sql("SELECT * FROM sparkPeopleTbl WHERE age > 37").map(row => "NAME: " + row(0)).collect

// COMMAND ----------

sql("SELECT * FROM sparkPeopleTbl WHERE age > 37").rdd.map(row => "NAME: " + row(0)).collect()

// COMMAND ----------

sql("SELECT * FROM sparkPeopleTbl WHERE age > 37")

// COMMAND ----------

// MAGIC %md ## Running SQL queries agains Parquet files directly

// COMMAND ----------

ls3("s3a://bigpicture-guild/nyctaxi/sample_1_month/parquet/")

// COMMAND ----------

// MAGIC %sql SELECT * FROM parquet.`s3a://bigpicture-guild/nyctaxi/sample_1_month/parquet/` WHERE trip_time_in_secs <= 60

// COMMAND ----------

// MAGIC %md # Conclusions

// COMMAND ----------

// MAGIC %md
// MAGIC ### RDDs
// MAGIC RDDs remain the core component for the native distributed collections.
// MAGIC But due to lack of built-in optimization, DFs and DSs should be prefered. 
// MAGIC 
// MAGIC 
// MAGIC ### DataFrames
// MAGIC **DataFrames** and **Spark SQL** are very flexible and bring built-in optimization also for dynamic languages like Python and R. Beside this, it allows to combine both *declarative* and *functional* way of working with structured data.
// MAGIC It's regarded to be most stable and flexible API.
// MAGIC 
// MAGIC <img src='https://databricks.com/wp-content/uploads/2015/02/Screen-Shot-2015-02-16-at-9.46.39-AM-1024x457.png' style='width:400px'>
// MAGIC 
// MAGIC 
// MAGIC ### Datasets
// MAGIC **Datasets** unify the best from both worlds: **type safety** from **RDDs** and **built-in optimization** available for **DataFrames**. 
// MAGIC But it is in experimental phase and can be used with Scala, Java and Python. DSs allow even further optimizations (memory compaction + faster serialization using encoders).
// MAGIC DataSets are not yet mature, but it is expected, that it will quickly become a common way to work with structured data, and Spark plans to converge the APIs even further.
// MAGIC 
// MAGIC Here some benchmarks of DataSets:
// MAGIC 
// MAGIC <img src='https://databricks.com/wp-content/uploads/2016/01/Distributed-Wordcount-Chart-1024x371.png' style='width:400px'>
// MAGIC <img src='https://databricks.com/wp-content/uploads/2016/01/Memory-Usage-when-Caching-Chart-1024x359.png?noresize' style='width:400px'>
// MAGIC <img src='https://databricks.com/wp-content/uploads/2016/01/Serialization-Deserialization-Performance-Chart-1024x364.png?noresize' style='width:400px'>

// COMMAND ----------

// MAGIC %md #Transformation of Data Types in Spark

// COMMAND ----------

// MAGIC %md  <p style="text-align:center;"><img src='https://s3.eu-central-1.amazonaws.com/spark-meetup/streaming/spark-datatype-conversion/demo2.png' style='width:500px'></p>
