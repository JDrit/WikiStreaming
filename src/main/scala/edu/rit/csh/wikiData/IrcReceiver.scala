package edu.rit.csh.wikiData

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/** Custom IRC receiver that creates IRC bots to listen on Wikipedia edit channels */
private class IrcReceiver(server: String, channels: List[String], storageLevel: StorageLevel)
  extends Receiver[Edit](storageLevel) with Logging {

  /** creates multiple bots so that they do not get blocked from the server */
  lazy val bots = channels.grouped(60).map(group => new IrcBot(this, group, server))

  override def onStart(): Unit = bots.foreach(_.start())

  override def onStop(): Unit = bots.foreach(_.disconnect())
}
