// Databricks notebook source exported at Tue, 19 Apr 2016 14:16:25 UTC
// MAGIC %md
// MAGIC 
// MAGIC # Twitter Hashtag Trends
// MAGIC 
// MAGIC Using Twitter Streaming is a great way to learn Spark Streaming if you don't have your streaming datasource and want a great rich input dataset to try Spark Streaming transformations on.
// MAGIC 
// MAGIC In this example, we show how to calculate the top hashtags seen in the last X window of time every Y time unit, and how to keep state across all windows

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

// COMMAND ----------

System.setProperty("twitter4j.oauth.consumerKey", "")
System.setProperty("twitter4j.oauth.consumerSecret", "")
System.setProperty("twitter4j.oauth.accessToken", "")
System.setProperty("twitter4j.oauth.accessTokenSecret", "")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Configuration

// COMMAND ----------

val checkpointDir = "/twitter"
// Recompute the top hashtags every N seconds
val batchInterval = Seconds(10)
// Compute the top hashtags for the last N seconds
val windowLength = Seconds(30)
// Wait this many seconds before stopping the streaming job
val timeoutJobLength = Minutes(5)

// COMMAND ----------

// MAGIC %md Clean up any old files. (If you want to actually start from checkpoint data, do not run this cell)

// COMMAND ----------

dbutils.fs.rm(checkpointDir, true)

// COMMAND ----------

// A mapping function that maintains an integer state word a given word
val mappingFunction = (word: String, one: Option[Int], state: State[Int]) => {
  val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
  val output = (word, sum)
  state.update(sum)
  output
}

val spec = StateSpec.function(mappingFunction).numPartitions(10)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Running the Twitter Streaming job

// COMMAND ----------

// MAGIC %md Create the function to that creates the Streaming Context and sets up the streaming job.

// COMMAND ----------

var newContextCreated = false
var num = 0

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  // Create a Spark Streaming Context.
  val ssc = new StreamingContext(sc, batchInterval)
  // Create a Twitter Stream for the input source. 
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth)
  
  // Parse the tweets and gather the hashTags.
  val hashTagStream = twitterStream.map(_.getText)
    .flatMap(_.split(" "))
    .filter(_.startsWith("#"))
  
  // Compute the counts of each hashtag by window.
  val windowedhashTagCountStream = hashTagStream.map((_, 1))
    .reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, batchInterval)
    .mapWithState(spec)

  // For each window, calculate the top hashtags for that time period.
  windowedhashTagCountStream.foreachRDD(hashTagCountRDD => {
    val df = hashTagCountRDD.toDF("hash", "count").sort($"count".desc)
    // Register table for analysis
    df.registerTempTable("hash_tag_count") 
    // Output top 10 hashtags to sdout
    val topEndpoints = df.take(10)
    println(s"------ TOP HASHTAGS for window ${num}")
    println(topEndpoints.mkString("\n"))
    num = num + 1
  })
  
  ssc.remember(Minutes(1))  // To make sure data is not deleted by the time we query it interactively
  
  newContextCreated = true
  ssc.checkpoint(checkpointDir)
  ssc
}

// COMMAND ----------

// MAGIC %md Create the StreamingContext using getActiveOrCreate, as required when starting a streaming job in Databricks.

// COMMAND ----------

@transient val ssc = StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Start the Spark Streaming Context

// COMMAND ----------

ssc.start()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### View the Results.

// COMMAND ----------

// MAGIC %sql select * from hash_tag_count limit 20

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Stop any active Streaming Contexts, but don't stop the spark contexts they are attached to.

// COMMAND ----------

StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------


