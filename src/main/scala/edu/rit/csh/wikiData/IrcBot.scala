package edu.rit.csh.wikiData

import java.sql.Timestamp

import scala.util.Random

import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver
import org.jibble.pircbot.PircBot

private case class Edit(channel: String, page: String, diff: String, username: String, comment: String, timestamp: Timestamp)

/** Custom IRC bot that parses edit messages from the IRC edit stream */
private class IrcBot(
    receiver: Receiver[Edit],
    channels: Seq[String],
    server: String,
    port: Int = 6667) extends PircBot with Logging with Serializable {

  val nick = s"jd-${Random.nextLong()}"

  def start(): Unit = {
    logInfo("starting IRC bot")
    setName(nick)
    setLogin(nick)
    setFinger("")
    setVerbose(true)
    connect(server, port)

    channels foreach joinChannel

    logInfo(s"IRC bot connected to $server")
  }

  override def onMessage(channel: String, sender: String, login: String, hostname: String, message: String): Unit = {
    processInput(message, channel, System.currentTimeMillis()) foreach receiver.store
  }

  private def processInput(line: String, channel: String, timestamp: Long): Option[Edit] = try {
    val extendedCode = (c: Char) => c < 32 || c > 127
    val input = line filterNot extendedCode
    val pageTitle = input.substring(input.indexOf("[[") + 4, input.indexOf("]]") - 2)
    val diff = input.substring(input.indexOf("]]") + 9).takeWhile(_ != ' ')
    val user = input.substring(input.indexOf(diff) + diff.length + 6, input.lastIndexOf(" 5* "))
    val comment = input.substring(input.indexOf(user) + user.length).dropWhile(_ != ')').substring(4)
    Some(Edit(channel, pageTitle, diff, user, comment, new Timestamp(timestamp)))
  } catch {
    case ex: Throwable => {
      logWarning(s"could not parse input line: $line, error $ex")
      None
    }
  }
}
