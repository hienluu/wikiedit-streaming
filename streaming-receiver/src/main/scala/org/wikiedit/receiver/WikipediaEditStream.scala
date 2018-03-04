package org.wikiedit.receiver

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import org.apache.spark.internal.Logging
import org.schwering.irc.lib.IRCConnection
import org.slf4j.{Logger, LoggerFactory};


class WikipediaEditStream(host:String, port:Int, queueSize:Int = 128)  {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  val editQueue: BlockingQueue[WikipediaEditEvent]  = new ArrayBlockingQueue(queueSize);

  val nick:String  = "spark-bot-" + (Math.random() * 1000).toInt
  val conn:IRCConnection = new IRCConnection(host, Array(port) , "", nick, nick, nick);

  conn.addIRCEventListener(new WikipediaIrcChannelListener(editQueue));
  conn.setEncoding("UTF-8");
  conn.setPong(true);
  conn.setColors(false);
  conn.setDaemon(true);
  conn.setName("WikipediaEditEventIrcStreamThread");

  def start()  {
    logger.info("starting the stream..")
    if (!conn.isConnected()) {
      conn.connect();
    }
  }

  def stop() {
    logger.info("stopping the stream..")
    if (conn.isConnected()) {
    }

    conn.interrupt();
    conn.join(5 * 1000);
  }

  def join(channel:String) {
    logger.info(s"joining channel $channel..")
    conn.send("JOIN " + channel);
  }

  def leave(channel:String) {
    logger.info(s"leaving channel $channel..")
    conn.send("PART " + channel);
  }

  def getEdits() : BlockingQueue[WikipediaEditEvent] = {
    return editQueue;
  }
}

object WikipediaEditStream {
  def apply(host:String, port:Int, queueSize:Int = 128) = {
    new WikipediaEditStream(host, port, queueSize)
  }
}