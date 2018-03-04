package org.wikiedit.receiver.structured_streaming

import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2
import org.json4s.DefaultFormats



/**
  * Created by hluu on 3/3/18.
  */
case class WikiEditOffset(offset:Long) extends v2.reader.streaming.Offset {
  implicit val defaultFormats: DefaultFormats = DefaultFormats
  override val json = offset.toString

  def +(increment: Long): WikiEditOffset = new WikiEditOffset(offset + increment)
  def -(decrement: Long): WikiEditOffset = new WikiEditOffset(offset - decrement)
}

object WikiEditOffset {
  def apply(offset: SerializedOffset) : WikiEditOffset = new WikiEditOffset(offset.json.toLong)

  def convert(offset: Offset): Option[WikiEditOffset] = offset match {
    case lo: WikiEditOffset => Some(lo)
    case so: SerializedOffset => Some(WikiEditOffset(so))
    case _ => None
  }
}