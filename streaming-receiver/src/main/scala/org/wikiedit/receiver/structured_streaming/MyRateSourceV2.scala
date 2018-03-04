package org.wikiedit.receiver.structured_streaming

import java.util.Optional

import scala.collection.JavaConverters._

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{RateStreamOffset, ValueRunTimeMsPair}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader,Offset}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}
import org.apache.spark.util.{ManualClock, SystemClock}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable

/**
  * Created by hluu on 3/3/18.
  */
class MyRateSourceV2 extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister with Logging {
  override def createMicroBatchReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): MicroBatchReader = {

    log.warn("****** createMicroBatchReader: ")
    new MyRateStreamMicroBatchReader(options)
  }

  override def shortName(): String = "my_ratev2"
}

class MyRateStreamMicroBatchReader(options: DataSourceOptions)
  extends MicroBatchReader {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  val clock = new MySystemClock /*{
    // The option to use a manual clock is provided only for unit testing purposes.
    if (options.get("useManualClock").orElse("false").toBoolean) new ManualClock
    else new SystemClock
  }*/

  private val numPartitions =
    options.get(MyRateStreamSourceV2.NUM_PARTITIONS).orElse("5").toInt
  private val rowsPerSecond =
    options.get(MyRateStreamSourceV2.ROWS_PER_SECOND).orElse("6").toLong

  // The interval (in milliseconds) between rows in each partition.
  // e.g. if there are 4 global rows per second, and 2 partitions, each partition
  // should output rows every (1000 * 2 / 4) = 500 ms.
  private val msPerPartitionBetweenRows = (1000 * numPartitions) / rowsPerSecond

  override def readSchema(): StructType = {
    StructType(
      StructField("timestamp", TimestampType, false) ::
        StructField("value", LongType, false) :: Nil)
  }

  val creationTimeMs = clock.getTimeMillis()

  private var start: RateStreamOffset = _
  private var end: RateStreamOffset = _

  override def setOffsetRange(
                               start: Optional[Offset],
                               end: Optional[Offset]): Unit = {
    this.start = start.orElse(
      MyRateStreamSourceV2.createInitialOffset(numPartitions, creationTimeMs))
      .asInstanceOf[RateStreamOffset]

    this.end = end.orElse {
      val currentTime = clock.getTimeMillis()
      RateStreamOffset(
        this.start.partitionToValueAndRunTimeMs.map {
          case startOffset @ (part, ValueRunTimeMsPair(currentVal, currentReadTime)) =>
            // Calculate the number of rows we should advance in this partition (based on the
            // current time), and output a corresponding offset.
            val readInterval = currentTime - currentReadTime
            val numNewRows = readInterval / msPerPartitionBetweenRows
            if (numNewRows <= 0) {
              startOffset
            } else {
              (part, ValueRunTimeMsPair(
                currentVal + (numNewRows * numPartitions),
                currentReadTime + (numNewRows * msPerPartitionBetweenRows)))
            }
        }
      )
    }.asInstanceOf[RateStreamOffset]
  }

  override def getStartOffset(): Offset = {
    if (start == null) throw new IllegalStateException("start offset not set")
    start
  }
  override def getEndOffset(): Offset = {
    if (end == null) throw new IllegalStateException("end offset not set")
    end
  }

  override def deserializeOffset(json: String): Offset = {
    RateStreamOffset(Serialization.read[Map[Int, ValueRunTimeMsPair]](json))
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    val startMap = start.partitionToValueAndRunTimeMs
    val endMap = end.partitionToValueAndRunTimeMs
    endMap.keys.toSeq.map { part =>
      val ValueRunTimeMsPair(endVal, _) = endMap(part)
      val ValueRunTimeMsPair(startVal, startTimeMs) = startMap(part)

      val packedRows = mutable.ListBuffer[(Long, Long)]()
      var outVal = startVal + numPartitions
      var outTimeMs = startTimeMs
      while (outVal <= endVal) {
        packedRows.append((outTimeMs, outVal))
        outVal += numPartitions
        outTimeMs += msPerPartitionBetweenRows
      }

      MyRateStreamBatchTask(packedRows).asInstanceOf[DataReaderFactory[Row]]
    }.toList.asJava
  }

  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}
}

case class MyRateStreamBatchTask(vals: Seq[(Long, Long)]) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new MyRateStreamBatchReader(vals)
}

class MyRateStreamBatchReader(vals: Seq[(Long, Long)]) extends DataReader[Row] {
  private var currentIndex = -1

  override def next(): Boolean = {
    // Return true as long as the new index is in the seq.
    currentIndex += 1
    currentIndex < vals.size
  }

  override def get(): Row = {
    Row(
      DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromMillis(vals(currentIndex)._1)),
      vals(currentIndex)._2)
  }

  override def close(): Unit = {}
}

object MyRateStreamSourceV2 {
  val NUM_PARTITIONS = "numPartitions"
  val ROWS_PER_SECOND = "rowsPerSecond"

  def createInitialOffset(numPartitions: Int, creationTimeMs: Long) = {
    RateStreamOffset(
      Range(0, numPartitions).map { i =>
        // Note that the starting offset is exclusive, so we have to decrement the starting value
        // by the increment that will later be applied. The first row output in each
        // partition will have a value equal to the partition index.
        (i,
          ValueRunTimeMsPair(
            (i - numPartitions).toLong,
            creationTimeMs))
      }.toMap)
  }
}

