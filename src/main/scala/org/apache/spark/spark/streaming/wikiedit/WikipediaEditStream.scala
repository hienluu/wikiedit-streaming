package org.apache.spark.spark.streaming.wikiedit

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import org.apache.spark.internal.Logging
import org.schwering.irc.lib.IRCConnection;


class WikipediaEditStream(host:String, port:Int) extends Logging {
  val editQueue: BlockingQueue[WikipediaEditEvent]  = new ArrayBlockingQueue(128);

  val nick:String  = "spark-bot-" + (Math.random() * 1000).toInt
  val conn:IRCConnection = new IRCConnection(host, Array(port) , "", nick, nick, nick);

  conn.addIRCEventListener(new WikipediaIrcChannelListener(editQueue));
  conn.setEncoding("UTF-8");
  conn.setPong(true);
  conn.setColors(false);
  conn.setDaemon(true);
  conn.setName("WikipediaEditEventIrcStreamThread");

  def start()  {
    if (!conn.isConnected()) {
      conn.connect();
    }
  }

  def stop() {
    if (conn.isConnected()) {
    }

    conn.interrupt();
    conn.join(5 * 1000);
  }

  def join(channel:String) {
    conn.send("JOIN " + channel);
  }

  def leave(channel:String) {
    conn.send("PART " + channel);
  }

  def getEdits() : BlockingQueue[WikipediaEditEvent] = {
    return editQueue;
  }
}
