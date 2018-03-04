package org.wikiedit.examples

/**
  * Created by hluu on 3/3/18.
  */

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.wikiedit.receiver.structured_streaming.{MyRateSourceProvider, WikiEditSourceProvider}

object WikiEditSourceExample {

  //private val SOURCE_PROVIDER_CLASS = MyRateSourceProvider.getClass.getCanonicalName
  private val SOURCE_PROVIDER_CLASS = WikiEditSourceProvider.getClass.getCanonicalName

  def main(args: Array[String]): Unit = {

    val providerClassName = SOURCE_PROVIDER_CLASS.substring(0, SOURCE_PROVIDER_CLASS.indexOf("$"))
    println(providerClassName)


    val spark = SparkSession
      .builder
      .appName("WikiEditStructuredStreamingExample")
      .master("local[*]")
      .getOrCreate()


    val enWikiEdit = spark.readStream.format(providerClassName).load
    enWikiEdit.printSchema()

    val enWikiEditQuery = enWikiEdit.writeStream.outputMode("append")
                                    .queryName("en_wikiedit").format("memory").start()

    var count:Long = 0
    while (count < 1) {
      println("**** Sleeping a little to wait for events to come in")
      Thread.sleep(TimeUnit.SECONDS.toMillis(5))
      count = spark.sql("select * from en_wikiedit").count()
    }


    println("**** There is data now.  Showing them")
    spark.sql("select * from en_wikiedit").show


    enWikiEditQuery.stop()
    spark.stop();

  }

  private def testLoadClass(className:String) : Unit = {
    val loader = Thread.currentThread().getContextClassLoader
    loader.loadClass(className).newInstance()
  }
}
