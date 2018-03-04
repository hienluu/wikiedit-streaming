package org.wikiedit.receiver.structured_streaming

import java.io.IOException
import java.util.concurrent.{BlockingQueue, TimeUnit}
import java.sql.Timestamp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.wikiedit.receiver.{WikipediaEditEvent, WikipediaEditStream}

import scala.collection.mutable.ListBuffer


/**
  * A source that generates stream of real-time wiki edits.
  */
class WikiEditSourceProvider extends StreamSourceProvider with DataSourceRegister {

  override def shortName(): String = "wikiedit"

  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) =
    (shortName(), WikiEditSourceProvider.SCHEMA)

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {
    val params = CaseInsensitiveMap(parameters)

    val host = params.get("host").map(_.toString).getOrElse("irc.wikimedia.org")
    val port = params.get("port").map(_.toInt).getOrElse(6667)
    val channel = params.get("channel").map(_.toString).getOrElse("#en.wikipedia")
    val queueSize = params.get("queueSize").map(_.toInt).getOrElse(128)

    if (queueSize < 1) {
      throw new IllegalArgumentException(
        s"Invalid value '${params("queueSize")}'. The option 'queueSize' " +
          "must be positive")
    }

    new WikiEditStreamSource(sqlContext, metadataPath, host, port, channel, queueSize)
  }
}

object WikiEditSourceProvider {
  val SCHEMA =
    StructType(StructField("timestamp", TimestampType) ::
               StructField("channel", StringType) ::
               StructField("title", StringType) ::
               StructField("diffUrl", StringType) ::
               StructField("user", StringType) ::
               StructField("byteDiff", IntegerType) ::
               StructField("summary", StringType) ::
               Nil)

  val VERSION = 1
}

/**
  * Contains the logic for reading wiki edits and managing the interaction with Spark Structured Streaming
  * engine.
  *
  * The implementation is modeled after {TextSocketSource} class
  *
  * @param sqlContext
  * @param metadataPath
  * @param host
  * @param port
  * @param channel
  * @param queueSize
  */
class WikiEditStreamSource(
                          sqlContext: SQLContext,
                          metadataPath: String,
                          host: String,
                          port: Int,
                          channel: String,
                          queueSize: Int) extends Source with Logging {

  override def schema: StructType = WikiEditSourceProvider.SCHEMA

  log.warn(s"**** WikiEditStreamSource host: $host, port: $port, channel: $channel, queueSize: $queueSize")

  private var currentOffset: LongOffset = new LongOffset(-1)
  private var lastReturnedOffset: LongOffset = new LongOffset(-2)
  private var lastOffsetCommitted : LongOffset = new LongOffset(-1)
  private var worker:Thread = null
  private var shouldStop:Boolean = false
  val editList:ListBuffer[WikipediaEditEvent] = new ListBuffer[WikipediaEditEvent]()


  var ircStream:WikipediaEditStream = new WikipediaEditStream(host, port, queueSize) with Serializable

  // kick off the thread to start receiving the events
  initialize()


  private def initialize(): Unit = synchronized {
    worker = new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() {
        receive()
      }
    }
    log.warn("*** starting worker thread")
    worker.start()
  }

  private def receive(): Unit = {
    ircStream.start();
    ircStream.join(channel);

    log.debug(s"joining channel $channel")

    while(!shouldStop) {
      // Query for the next edit event
      val edit: BlockingQueue[WikipediaEditEvent] = ircStream.getEdits();
      if (edit != null) {
        val wikiEdit: WikipediaEditEvent = edit.poll(100, TimeUnit.MILLISECONDS)

        if (wikiEdit != null) {
          editList.append(wikiEdit);
          currentOffset = currentOffset + 1
        }
      }
    }
  }

  /**
    * Return the current max offset
    * @return
    */
  override def getOffset: Option[Offset] = {
    if (currentOffset.offset == -1) {
      None
    } else {
      if (lastReturnedOffset.offset < currentOffset.offset) {
        log.debug(s"** getOffset($currentOffset)")
        lastReturnedOffset = currentOffset
      }
      Some(currentOffset)
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    log.debug(s"** getBatch($start, $end)")

    val startOrdinal =
      start.flatMap(LongOffset.convert).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset.toInt + 1

    val rawList = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      editList.slice(sliceStart, sliceEnd)
    }

    val rdd = sqlContext.sparkContext.parallelize(rawList).map { evt =>
      Row(new Timestamp(evt.timeStamp), evt.channel, evt.title, evt.diffUrl, evt.user,
        evt.byteDiff, evt.summary)
    }
    sqlContext.createDataFrame(rdd, schema)

  }

  override def commit(end: Offset) : Unit = {

    log.debug(s"** commit($end)")

    val newOffset = LongOffset.convert(end).getOrElse(
      sys.error(s"TextSocketStream.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    editList.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = {
    shouldStop = true
    if (ircStream != null) {
      try {
        ircStream.leave(channel)
        ircStream.stop()
      } catch {
        case e: IOException =>
      }
    }
  }

  override def toString: String = s"WikiEditStreamSource[host: $host, port: $port, channel: $channel]"
}