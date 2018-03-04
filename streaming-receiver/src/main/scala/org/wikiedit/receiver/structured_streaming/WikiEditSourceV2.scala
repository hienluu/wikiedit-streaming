package org.wikiedit.receiver.structured_streaming

import java.io.IOException
import java.sql.Timestamp
import java.util.Optional
import java.util.concurrent.{BlockingQueue, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types._
import org.wikiedit.receiver.{WikipediaEditEvent, WikipediaEditStream}

import scala.collection.mutable.ListBuffer

/**
  * A wiki edit source based on Apache Spark DataSource V2 API.  This requires Spark 2.3 to compile.
  *
  * May need to switch their new web service https://wikitech.wikimedia.org/wiki/EventStreams
  *
  *
  * Created by hluu on 3/3/18.
  */
class WikiEditSourceV2 extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister with Logging {
  override def createMicroBatchReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): MicroBatchReader = {
    new WikiEditStreamMicroBatchReader(options)
  }

  override def shortName(): String = "wikieditV2"
}

class WikiEditStreamMicroBatchReader(options: DataSourceOptions) extends MicroBatchReader with Logging {

  private val host = options.get(WikiEditSourceV2.HOST).orElse("irc.wikimedia.org")
  private val port = options.get(WikiEditSourceV2.PORT).orElse("6667").toInt
  private val channel = options.get(WikiEditSourceV2.CHANNEL).orElse("#en.wikipedia")
  private val queueSize = options.get(WikiEditSourceV2.QUEUE_SIZE).orElse("128").toInt
  private val numPartitions = options.get(WikiEditSourceV2.NUM_PARTITIONS).orElse("5").toInt
  private val debugLevel = options.get(WikiEditSourceV2.DEBUG_LEVEL).orElse("debug").toLowerCase

  private val NO_DATA_OFFSET = WikiEditOffset(-1)

  private var shouldStop:Boolean = false
  private var worker:Thread = null

  private val wikiEditList:ListBuffer[WikipediaEditEvent] = new ListBuffer[WikipediaEditEvent]()

  private var ircStream:WikipediaEditStream = null

  private var startOffset: WikiEditOffset = new WikiEditOffset(-1)
  private var endOffset: WikiEditOffset = new WikiEditOffset(-1)

  private var currentOffset: WikiEditOffset = new WikiEditOffset(-1)
  private var lastReturnedOffset: WikiEditOffset = new WikiEditOffset(-2)
  private var lastOffsetCommitted : WikiEditOffset = new WikiEditOffset(-1)

  private var incomingEventCounter = 0;

  // kick off a thread to start receiving the events
  initialize()

  private def initialize(): Unit = synchronized {
    logInputs()
    ircStream = WikipediaEditStream(host, port, queueSize)

    worker = new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() {
        receive()
      }
    }
    worker.start()
  }

  private def logInputs(): Unit = {
    log.warn(s"host: $host, port: $port, channel: $channel, queueSize: $queueSize, " +
      s"numPartitions: $numPartitions, debugLevel: $debugLevel")
  }
  private def internalLog(msg:String): Unit = {
    debugLevel match {
      case "warn" => log.warn(msg)
      case "info" => log.info(msg)
      case "debug" => log.debug(msg)
      case _ =>
    }
  }
  private def receive(): Unit = {
    ircStream.start();
    ircStream.join(channel);

    log.warn(s"joining channel $channel")

    while(!shouldStop) {
      // Query for the next edit event
      val edit: BlockingQueue[WikipediaEditEvent] = ircStream.getEdits();
      if (edit != null) {
        val wikiEdit: WikipediaEditEvent = edit.poll(100, TimeUnit.MILLISECONDS)

        if (wikiEdit != null) {
          wikiEditList.append(wikiEdit);
          currentOffset = currentOffset + 1
          incomingEventCounter = incomingEventCounter + 1;
        }
      }
    }
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    synchronized {
      val startOrdinal = startOffset.offset.toInt + 1
      val endOrdinal = endOffset.offset.toInt + 1

      internalLog(s"createDataReaderFactories: sOrd: $startOrdinal, eOrd: $endOrdinal, " +
        s"lastOffsetCommitted: $lastOffsetCommitted")

      val newBlocks = synchronized {
        val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
        val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
        assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
        wikiEditList.slice(sliceStart, sliceEnd)
      }

      newBlocks.grouped(numPartitions).map { block =>
        new WikiEditStreamBatchTask(block).asInstanceOf[DataReaderFactory[Row]]
      }.toList.asJava
    }
  }

  override def setOffsetRange(start: Optional[Offset],
                              end: Optional[Offset]): Unit = {
    if (start.isPresent && start.get().asInstanceOf[WikiEditOffset].offset != currentOffset.offset) {
      internalLog(s"setOffsetRange: start: $start, end: $end currentOffset: $currentOffset")
    }

    this.startOffset = start.orElse(NO_DATA_OFFSET).asInstanceOf[WikiEditOffset]
    this.endOffset = end.orElse(currentOffset).asInstanceOf[WikiEditOffset]
  }

  override def getStartOffset(): Offset = {
    internalLog("getStartOffset was called")
    if (startOffset.offset == -1) {
      throw new IllegalStateException("startOffset is -1")
    }
    startOffset
  }

  override def getEndOffset(): Offset = {
    if (endOffset.offset == -1) {
      currentOffset
    } else {

      if (lastReturnedOffset.offset < endOffset.offset) {
        internalLog(s"** getEndOffset => $endOffset)")
        lastReturnedOffset = endOffset
      }

      endOffset
    }

  }

  override def deserializeOffset(json: String): Offset = {
    WikiEditOffset(json.toLong)
  }


  override def readSchema(): StructType = {
    WikiEditSourceV2.SCHEMA
  }

  override def commit(end: Offset): Unit = {
    internalLog(s"** commit($end) lastOffsetCommitted: $lastOffsetCommitted")

    val newOffset = WikiEditOffset.convert(end).getOrElse(
      sys.error(s"WikiEditStreamMicroBatchReader.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    wikiEditList.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  override def stop(): Unit = {
    log.warn(s"There is a total of $incomingEventCounter events that came in")
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
}

object WikiEditSourceV2 {
  val HOST = "host"
  val PORT = "port"
  val CHANNEL = "channel"
  val QUEUE_SIZE = "queueSize"
  val NUM_PARTITIONS = "numPartitions"
  val DEBUG_LEVEL = "debugLevel"

  val SCHEMA =
    StructType(StructField("timestamp", TimestampType) ::
      StructField("channel", StringType) ::
      StructField("title", StringType) ::
      StructField("diffUrl", StringType) ::
      StructField("user", StringType) ::
      StructField("byteDiff", IntegerType) ::
      StructField("summary", StringType) ::
      Nil)
}

class WikiEditStreamBatchTask(wikiEditEvents:ListBuffer[WikipediaEditEvent])
  extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new WikiEditStreamBatchReader(wikiEditEvents)
}

class WikiEditStreamBatchReader(wikiEditEvents:ListBuffer[WikipediaEditEvent]) extends DataReader[Row] {
  private var currentIdx = -1

  override def next(): Boolean = {
    // Return true as long as the new index is in the seq.
    currentIdx += 1
    currentIdx < wikiEditEvents.size
  }

  override def get(): Row = {
    val evt = wikiEditEvents(currentIdx)
    Row(new Timestamp(evt.timeStamp), evt.channel, evt.title, evt.diffUrl, evt.user,
      evt.byteDiff, evt.summary)
  }

  override def close(): Unit = {}
}