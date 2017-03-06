package org.apache.spark.spark.streaming.wikiedit.example

import org.apache.spark.spark.streaming.wikiedit.WikiEditStreamReceiver

object Helloworld {
  def main(args: Array[String]) {
    println("hello world!!")
    val receiver = new WikiEditStreamReceiver()

  }
}
