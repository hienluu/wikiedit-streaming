package org.wikiedit.receiver

import java.util.concurrent.BlockingQueue

import com.google.common.base.Preconditions
import org.schwering.irc.lib.{IRCEventListener, IRCModeParser, IRCUser}
import org.slf4j.{Logger, LoggerFactory}

class WikipediaIrcChannelListener(editQueue:BlockingQueue[WikipediaEditEvent]) extends IRCEventListener {
  //Preconditions.checkNotNull(editQueue, "editQueue can't be null")

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  override def onPrivmsg(target: String, ircUser: IRCUser, message: String) = {

    val event:Option[WikipediaEditEvent] = WikipediaEditEventUtil.fromRawEvent(
              System.currentTimeMillis(), target, message)

    if (event.isDefined) {
      editQueue.add(event.get)
    }
  }

  @Override
  def onRegistered() {
    logger.debug("Connected.");
  }

  @Override
  def onDisconnected() {
    logger.debug("Disconnected.");
  }

  @Override
  def onError(msg:String) {
    logger.error(s"Error: '{$msg}'.");
  }

  @Override
  def onError(num:Int,  msg:String) {
    logger.error(s"Error #{$num}: '{$msg}'.");
  }

  @Override
  def onInvite(chan:String, user:IRCUser,  passiveNick:String) {
    logger.debug(s"[{$chan}]: {${user.getNick}} invites {$passiveNick}.");
  }

  @Override
  def onJoin(chan:String,  user:IRCUser) {
    logger.debug(s"[{$chan}]: {${user.getNick}} joins.");
  }

  @Override
  def onKick( chan:String,  user:IRCUser,  passiveNick:String,  msg:String) {
    logger.debug(s"[{$chan}]: {${user.getNick}} kicks {$passiveNick}.");
  }

  @Override
  def onMode(chan:String,  user:IRCUser,  modeParser:IRCModeParser) {
    logger.debug(s"[{$chan}]: mode '{$modeParser.getLine}'.");
  }

  @Override
  def onMode( user:IRCUser,  passiveNick:String,  mode:String) {
    logger.debug(s"{${user.getNick}} sets modes {$mode} ({$passiveNick}).")
  }

  @Override
  def onNick( user:IRCUser,  newNick:String) {
    logger.debug(s"{${user.getNick}} is now known as {$newNick}.")
  }

  @Override
  def onNotice( target:String,  user:IRCUser,  msg:String) {
    logger.debug(s"[{$target}] {${user.getNick}} (notice): {$msg}.");
  }

  @Override
  def onPart( chan:String,  user:IRCUser,  msg:String) {
    logger.debug(s"[{$chan}] {${user.getNick}} {$msg} parts.");
  }

  @Override
  def onPing( ping:String) {
  }

  @Override
  def onQuit( user:IRCUser,  msg:String) {
    logger.debug(s"Quit: {${user.getNick}}.", user.getNick());
  }

  @Override
  def onReply( num:Int,  value:String,  msg:String) {
    logger.debug(s"Reply #{$num}: {$value} {$msg}.");
  }

  @Override
  def onTopic( chan:String,  user:IRCUser,  topic:String) {
    logger.debug(s"[{$chan}] {${user.getNick}} changes topic into {$topic}.");
  }

  @Override
  def unknown( prefix:String, command:String, middle:String, trailing:String) {
    logger.warn("UNKNOWN: " + prefix + " " + command + " " + middle + " " + trailing);
  }

}
