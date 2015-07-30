package edu.rit.csh.wikiData

import java.sql.Timestamp

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.pircbotx.hooks.events.MessageEvent
import org.pircbotx.{PircBotX, Configuration}
import org.pircbotx.hooks.ListenerAdapter
import org.pircbotx.hooks.types.GenericMessageEvent

import scala.concurrent.Future
import scala.util.Random

case class Edit(channel: String,
                page: String,
                diff: String,
                username: String,
                comment: String,
                timestamp: Timestamp = new Timestamp(System.currentTimeMillis()))

private class IrcReceiver(server: String, channels: List[String], storageLevel: StorageLevel)
  extends Receiver[Edit](storageLevel) with Logging {

  private val nick = s"jd-${Random.nextInt()}"

  private def generateConf(): Configuration.Builder[PircBotX] = new Configuration.Builder()
      .setName(nick)
      .setLogin(nick)
      .setRealName(nick)
      .setServerHostname(server)
      .addListener(new ListenerAdapter[PircBotX] {
      val extendedCode = (c: Char) => c < 32 || c > 127

      override def onMessage(event: MessageEvent[PircBotX]): Unit = {
        val line = event.getMessage.filterNot(extendedCode)

        processInput(line, event.getChannel.getName.substring(1), event.getTimestamp) match {
          case Some(e) => store(e)
          case None => logInfo(s"could not parse message ${line}")
        }
      }
    })

  private lazy val bots: List[PircBotX] = channels.grouped(30).map { group =>
    val conf = generateConf()
    group.foreach(channel => conf.addAutoJoinChannel(channel))
    new PircBotX(conf.buildConfiguration())
  }.toList

  override def onStart(): Unit = {
    new Thread {
      override def run(): Unit = {
        bots.foreach(_.startBot())
      }
    }.run()
  }

  override def onStop(): Unit = {
    logInfo("shutting down IRC receiver")
  }


  private def processInput(input: String, channel: String, timestamp: Long): Option[Edit] = {
    try {
      val pageTitle = input.substring(input.indexOf("[[") + 4, input.indexOf("]]") - 2)
      val diff = input.substring(input.indexOf("]]") + 9).takeWhile(_ != ' ')
      val user = input.substring(input.indexOf(diff) + diff.length + 6, input.lastIndexOf(" 5* "))
      val comment = input.substring(input.indexOf(user) + user.length).dropWhile(_ != ')').substring(4)
      Some(Edit(channel, pageTitle, diff, user, comment, new Timestamp(timestamp)))
    } catch {
      case ex: Throwable => {
        logWarning(s"could not parse input line: $input, error $ex")
        None
      }
    }
  }
}
