package org.wikiedit.examples

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.wikiedit.receiver.structured_streaming.WikiEditSourceV2

/**
  * An exmaple to exercise WikiEditSourceV2
  *
  * See here for a list of channels - https://meta.wikimedia.org/wiki/IRC/Channels
  *
  * Created by hluu on 3/4/18.
  */
object WikiEditSourceV2Example {
  private val SOURCE_PROVIDER_CLASS = WikiEditSourceV2.getClass.getCanonicalName

  def main(args: Array[String]): Unit = {

    val providerClassName = SOURCE_PROVIDER_CLASS.substring(0, SOURCE_PROVIDER_CLASS.indexOf("$"))
    println(providerClassName)


    val spark = SparkSession
      .builder
      .appName("WikiEditSourceV2Example")
      .master("local[*]")
      .getOrCreate()


    val wikiEdit = spark.readStream.format(providerClassName)
                        .option("channel", "#fr.wikipedia")
                        .option("debugLevel", "info")
                        .load

    wikiEdit.printSchema()

    val wikiEditQuery = wikiEdit.writeStream.outputMode("append")
      .queryName("wikiedit").format("memory").start()

    var counter:Long = 0
    while (counter < 1) {
      println("**** Sleeping a little to wait for events to come in")
      Thread.sleep(TimeUnit.SECONDS.toMillis(5))
      counter = spark.sql("select * from wikiedit").count()
    }


    println("**** There is data now.  Showing them")
    spark.sql("select * from wikiedit").show

    Thread.sleep(TimeUnit.SECONDS.toMillis(3))

    spark.sql("select * from wikiedit").show

    val wikiEditCount = spark.sql("select * from wikiedit").count;
    println(s"There are total of $wikiEditCount in memory table");

    wikiEditQuery.stop()

    for(qs <- spark.streams.active) {
      println(s"Stop streaming query: ${qs.name} - active: ${qs.isActive}")
      if (qs.isActive) {
        qs.stop
      }
    }

    spark.stop();
  }
}
