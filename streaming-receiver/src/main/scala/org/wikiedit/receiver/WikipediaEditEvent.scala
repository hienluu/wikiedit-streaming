package org.wikiedit.receiver

import java.util.regex.Pattern
import java.util.regex.Matcher

case class WikipediaEditEvent(timeStamp:Long,
                              channel:String,
                              title:String,
                              diffUrl:String,
                              user:String,
                              byteDiff:Int,
                              summary:String,
                              isMinor:Boolean,
                              isNew:Boolean,
                              isUnpatrolled:Boolean,
                              isBotEdit:Boolean,
                              isSpecial:Boolean,
                              isTalk:Boolean,
                              var flag:Int = 0) {
  if (channel == null || title == null || diffUrl == null ||
    user == null || summary == null) {
    throw new NullPointerException();
  }

  flag = WikipediaEditEventUtil.getFlags(isMinor,
    isNew,
    isUnpatrolled,
    isBotEdit,
    isSpecial,
    isTalk)
}

object WikipediaEditEventUtil {


  val IS_MINOR:Byte =  1
  val IS_NEW:Byte = 2
  val IS_UNPATROLLED:Byte = 4
  val IS_BOT_EDIT:Byte = 8
  val IS_SPECIAL:Byte = 16
  val IS_TALK:Byte = 32


  val  pattern:Pattern = Pattern.compile("\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)");

  def fromRawEvent(timeStamp:Long, channel:String, rawEvent:String): Option[WikipediaEditEvent] = {
    val matcher:Matcher = pattern.matcher(rawEvent)

    if (matcher.find() && matcher.groupCount() == 6) {
      val title: String = matcher.group(1);
      val flags: String = matcher.group(2);
      val difUrl: String = matcher.group(3);
      val user: String = matcher.group(4);
      val byteDiff: Int = Integer.parseInt(matcher.group(5));
      val summary: String = matcher.group(6);

      val isMinor: Boolean = flags.contains("M");
      val isNew: Boolean = flags.contains("N");
      val isUnpatrolled: Boolean = flags.contains("!");
      val isBotEdit: Boolean = flags.contains("B");
      val isSpecial: Boolean = title.startsWith("Special:");
      val isTalk: Boolean = title.startsWith("Talk:");


      Some(WikipediaEditEvent(timeStamp, channel, title, difUrl,
        user, byteDiff, summary, isMinor, isNew, isUnpatrolled,
        isBotEdit, isSpecial, isTalk))
    } else {
      None
    }
  }

  def getFlags(isMinor:Boolean,
               isNew:Boolean,
               isUnpatrolled:Boolean,
               isBotEdit:Boolean,
               isSpecial:Boolean,
               isTalk:Boolean) : Byte = {

    var flag:Byte = 0

    flag = (if (isMinor)  IS_MINOR else flag | flag).toByte
    flag = (if (isNew) IS_NEW else flag | flag).toByte
    flag = (if (isUnpatrolled)  IS_UNPATROLLED else flag | flag).toByte
    flag = (if (isBotEdit) IS_BOT_EDIT else flag | flag).toByte
    flag = (if (isSpecial) IS_SPECIAL else flag | flag).toByte
    flag = (if (isTalk) IS_TALK else flag | flag).toByte

    return flag;
  }
}