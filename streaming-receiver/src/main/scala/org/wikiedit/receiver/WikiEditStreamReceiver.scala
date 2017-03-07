package org.wikiedit.receiver

import java.util.concurrent.{BlockingQueue, TimeUnit}

import org.slf4j.{Logger, LoggerFactory}


import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class WikiEditStreamReceiver(host:String = "irc.wikimedia.org", port:Int = 6667,
                             channel:String = "#en.wikipedia")
  extends Receiver[WikipediaEditEvent](StorageLevel.MEMORY_AND_DISK_2)  {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  lazy val ircStream:WikipediaEditStream = new WikipediaEditStream(host, port) with Serializable

  def onStart() {

    logger.info("Starting " + this.getClass.getName)
    ///Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
    if (ircStream != null) {
      ircStream.leave(channel);
      ircStream.stop();
    }
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  private def receive(): Unit = {
    ircStream.start();
    ircStream.join(channel);

    logger.info(s"joining channel $channel")

    while(!isStopped) {
      // Query for the next edit event
      val edit: BlockingQueue[WikipediaEditEvent] = ircStream.getEdits();
      if (edit != null) {
        val wikiEdit: WikipediaEditEvent = edit.poll(100, TimeUnit.MILLISECONDS)

        if (wikiEdit != null) {
          //logWarning("storing: " + wikiEdit.toString)
          store(wikiEdit)
        }
      } else {
        logger.warn("**** edit queue is null ******");
      }
    }
  }

}
