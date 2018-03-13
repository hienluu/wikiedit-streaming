package org.wikiedit.examples

import org.apache.spark.sql.SparkSession
import org.twitterstreaming.receiver.TwitterStreamingSource


/**
  * Created by hluu on 3/11/18.
  */
object TwitterSourceExample {
  private val SOURCE_PROVIDER_CLASS = TwitterStreamingSource.getClass.getCanonicalName

  def main(args: Array[String]): Unit = {
    println("TwitterSourceExample")

    val providerClassName = SOURCE_PROVIDER_CLASS.substring(0, SOURCE_PROVIDER_CLASS.indexOf("$"))
    println(providerClassName)

    if (args.length != 4) {
      println("Usage: <consumer key>, <consumer secret> <access token> <access token secret>")
      sys.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)


    val spark = SparkSession
      .builder
      .appName("TwitterSourceExample")
      .master("local[*]")
      .getOrCreate()

    val tweetDF = spark.readStream
                       .format(providerClassName)
                       .option(TwitterStreamingSource.CONSUMER_KEY, consumerKey)
                       .option(TwitterStreamingSource.CONSUMER_SECRET, consumerSecret)
                       .option(TwitterStreamingSource.ACCESS_TOKEN, accessToken)
                       .option(TwitterStreamingSource.ACCESS_TOKEN_SECRET, accessTokenSecret)
                         .load()

    tweetDF.printSchema()

    val tweetQS = tweetDF.writeStream.format("console").start()

    Thread.sleep(1000 * 35)

    tweetQS.stop();

  }
}
