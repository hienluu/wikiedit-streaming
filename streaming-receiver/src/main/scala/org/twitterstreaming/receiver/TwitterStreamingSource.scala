package org.twitterstreaming.receiver


import java.io.IOException
import java.util.Optional
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
import java.sql.Timestamp

import scala.collection.JavaConverters._
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.Status
import twitter4j.StatusListener
import twitter4j.StatusDeletionNotice
import twitter4j.StallWarning
import twitter4j.TwitterStream
import twitter4j.TwitterStreamFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types._


import scala.collection.mutable.ListBuffer


/**
  * Created by hluu on 3/10/18.
  */
class TwitterStreamingSource extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister with Logging {

  override def createMicroBatchReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): MicroBatchReader = {
    new TwitterStreamMicroBatchReader(options)
  }

  override def shortName(): String = "twitter"
}

class TwitterStreamMicroBatchReader(options: DataSourceOptions) extends MicroBatchReader with Logging {
  private val consumerKey = options.get(TwitterStreamingSource.CONSUMER_KEY).orElse("")
  private val consumerSecret = options.get(TwitterStreamingSource.CONSUMER_SECRET).orElse("")
  private val accessToken = options.get(TwitterStreamingSource.ACCESS_TOKEN).orElse("")
  private val accessTokenSecret = options.get(TwitterStreamingSource.ACCESS_TOKEN_SECRET).orElse("")
  private val numPartitions = options.get(TwitterStreamingSource.NUM_PARTITIONS).orElse("5").toInt
  private val queueSize = options.get(TwitterStreamingSource.QUEUE_SIZE).orElse("512").toInt

  private val debugLevel = options.get(TwitterStreamingSource.DEBUG_LEVEL).orElse("debug").toLowerCase

  private val NO_DATA_OFFSET = TwitterOffset(-1)



  private var startOffset: TwitterOffset = new TwitterOffset(-1)
  private var endOffset: TwitterOffset = new TwitterOffset(-1)

  private var currentOffset: TwitterOffset = new TwitterOffset(-1)
  private var lastReturnedOffset: TwitterOffset = new TwitterOffset(-2)
  private var lastOffsetCommitted : TwitterOffset = new TwitterOffset(-1)

  private var incomingEventCounter = 0;
  private var stopped:Boolean = false

  private var twitterStream:TwitterStream = null
  private var worker:Thread = null

  private val tweetList:ListBuffer[Status] = new ListBuffer[Status]()
  private var tweetQueue:BlockingQueue[Status] = null

  initialize()

  private def initialize(): Unit = synchronized {

    if (consumerKey == "" || consumerSecret == "" || accessToken == "" || accessTokenSecret == "") {
      throw new IllegalStateException("One or more pieces of required OAuth info. is missing." +
        s" Make sure the following are provided ${TwitterStreamingSource.CONSUMER_KEY}," +
        s"${TwitterStreamingSource.CONSUMER_SECRET} ${TwitterStreamingSource.ACCESS_TOKEN} " +
        s"${TwitterStreamingSource.ACCESS_TOKEN_SECRET}")
    }

    tweetQueue = new ArrayBlockingQueue(queueSize)

    val configBuilder:ConfigurationBuilder  = new ConfigurationBuilder()
    configBuilder.setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
    configBuilder.setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

    val twitterAuth = new OAuthAuthorization(configBuilder.build())
    twitterStream = new TwitterStreamFactory().getInstance(twitterAuth)

    twitterStream.addListener(new StatusListener {
      def onStatus(status: Status): Unit = {
        tweetQueue.add(status)
      }
      // Unimplemented
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(i: Int) {}
      def onScrubGeo(l: Long, l1: Long) {}
      def onStallWarning(stallWarning: StallWarning) {}
      def onException(e: Exception) {
        /*if (!stopped) {
          restart("Error receiving tweets", e)
        }*/
      }
    })

    worker = new Thread("Tweet Worker") {
      setDaemon(true)
      override def run() {
        receive()
      }
    }
    worker.start()

    // start receiving tweets
    twitterStream.sample()
  }

  private def receive(): Unit = {

    while(!stopped) {
        // poll tweets from queue
        val tweet:Status = tweetQueue.poll(100, TimeUnit.MILLISECONDS)

        if (tweet != null) {

          tweetList.append(tweet);
          currentOffset = currentOffset + 1

          incomingEventCounter = incomingEventCounter + 1;
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
        tweetList.slice(sliceStart, sliceEnd)
      }

      newBlocks.grouped(numPartitions).map { block =>
        new TweetStreamBatchTask(block).asInstanceOf[DataReaderFactory[Row]]
      }.toList.asJava
    }
  }

  override def setOffsetRange(start: Optional[Offset],
                              end: Optional[Offset]): Unit = {

    if (start.isPresent && start.get().asInstanceOf[TwitterOffset].offset != currentOffset.offset) {
      internalLog(s"setOffsetRange: start: $start, end: $end currentOffset: $currentOffset")
    }

    this.startOffset = start.orElse(NO_DATA_OFFSET).asInstanceOf[TwitterOffset]
    this.endOffset = end.orElse(currentOffset).asInstanceOf[TwitterOffset]
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

  override def commit(end: Offset): Unit = {
    internalLog(s"** commit($end) lastOffsetCommitted: $lastOffsetCommitted")

    val newOffset = TwitterOffset.convert(end).getOrElse(
      sys.error(s"TwitterStreamMicroBatchReader.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    tweetList.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  override def stop(): Unit = {
    log.warn(s"There is a total of $incomingEventCounter events that came in")
    stopped = true
    if (twitterStream != null) {
      try {
        twitterStream.shutdown()
      } catch {
        case e: IOException =>
      }
    }
  }

  override def deserializeOffset(json: String): Offset = {
    TwitterOffset(json.toLong)
  }
  override def readSchema(): StructType = {
    TwitterStreamingSource.SCHEMA
  }

  private def internalLog(msg:String): Unit = {
    debugLevel match {
      case "warn" => log.warn(msg)
      case "info" => log.info(msg)
      case "debug" => log.debug(msg)
      case _ =>
    }
  }
}

object TwitterStreamingSource {

  val CONSUMER_KEY = "consumerKey"
  val CONSUMER_SECRET = "consumerSecret"
  val ACCESS_TOKEN = "accessToken"
  val ACCESS_TOKEN_SECRET = "accessTokenSecret"
  val DEBUG_LEVEL = "debugLevel"
  val NUM_PARTITIONS = "numPartitions"
  val QUEUE_SIZE = "queueSize"


  val SCHEMA =
    StructType(
      StructField("text", StringType) ::
      StructField("user", StringType) ::
      StructField("userLang", StringType) ::
      StructField("createdDate", TimestampType) ::
      StructField("isRetweeted", BooleanType) ::
      Nil)
}

class TweetStreamBatchTask(tweetList:ListBuffer[Status])
  extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new TweetStreamBatchReader(tweetList)
}

class TweetStreamBatchReader(tweetList:ListBuffer[Status]) extends DataReader[Row] {
  private var currentIdx = -1

  override def next(): Boolean = {
    // Return true as long as the new index is in the seq.
    currentIdx += 1
    currentIdx < tweetList.size
  }

  override def get(): Row = {
    val tweet = tweetList(currentIdx)
    Row(tweet.getText, tweet.getUser.getScreenName, tweet.getUser.getLang, new Timestamp(tweet.getCreatedAt.getTime),
        tweet.isRetweeted)
  }

  override def close(): Unit = {}
}
