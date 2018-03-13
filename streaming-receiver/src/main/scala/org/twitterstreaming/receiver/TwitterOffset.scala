package org.twitterstreaming.receiver

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2
import org.json4s.DefaultFormats


/**
  * Created by hluu on 3/10/18.
  */
case class TwitterOffset(offset:Long) extends v2.reader.streaming.Offset  {
  implicit val defaultFormats: DefaultFormats = DefaultFormats
  override val json = offset.toString

  def +(increment: Long): TwitterOffset = new TwitterOffset(offset + increment)
  def -(decrement: Long): TwitterOffset = new TwitterOffset(offset - decrement)
}

object TwitterOffset {
  def apply(offset: SerializedOffset) : TwitterOffset = new TwitterOffset(offset.json.toLong)

  def convert(offset: Offset): Option[TwitterOffset] = offset match {
    case lo: TwitterOffset => Some(lo)
    case so: SerializedOffset => Some(TwitterOffset(so))
    case _ => None
  }
}