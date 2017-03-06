package org.apache.spark.spark.streaming.wikiedit

import org.apache.spark.streaming.StreamingContext

class WikiEditStreamUtil {
  def createStream(ssc:StreamingContext, host:String = "irc.wikimedia.org", port:Int = 6667,
                   channel:String = "#en.wikipedia"): Unit = {

  }

}
