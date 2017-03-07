package org.wikiedit.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.wikiedit.receiver.{WikiEditStreamReceiver, WikipediaEditEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WikepediaEditStreamExample {
  def main(args: Array[String]) {

    setStreamingLogLevels

    val sparkConf = new SparkConf().setAppName("WikepediaEditStreamExample").setMaster("local[*]")

    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    } else {
      println("master as: " + sparkConf.get("spark.master"))
    }

    val spark = SparkSession.builder().appName("WikepediaEditStreamExample").config(sparkConf).getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val wikiEditStreamReceiver:WikiEditStreamReceiver = new WikiEditStreamReceiver()
    val stream = ssc.receiverStream(wikiEditStreamReceiver)

    val editEvents = stream.map(e => e)


    editEvents.foreachRDD { (rdd:RDD[WikipediaEditEvent]) =>
        import spark.implicits._

        if (!rdd.isEmpty()) {
          println("**** yeah!!!, RDD has some data")
          val eventDF = rdd.toDS()

          //eventDF.createOrReplaceTempView("events")

          eventDF.show()
        } else {
          println("rdd is empty, no data yet")
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      println("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
