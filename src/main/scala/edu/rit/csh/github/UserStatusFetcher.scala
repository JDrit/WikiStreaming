package edu.rit.csh.github

import edu.rit.csh.github.EventType.EventType
import org.apache.spark.Logging
import org.json4s._
import org.json4s.native.JsonParser._

import scala.concurrent.duration._
import scala.util.Random
import scalaj.http.Http

class UserStatusFetcher(username: String, apiToken: String, receiver: GithubReceiver)
    extends Runnable with Logging {

  implicit lazy val formats = DefaultFormats
  private val url = s"https://api.github.com/users/$username/events"
  private val pollTime = 2.minutes.toMillis
  var lastId = -1L


  def convertEvent(e: EventParse): Event = Event(e.id.toLong, username, EventType.withName(e.`type`))

  /**
   * Queries for the user's event stream and sends the receiver all the new events
   * @param first if this is the first time it is running, if it is all events should be returned
   */
  private def fetchStatus(first: Boolean = false): Unit = {
    val response = Http(url).param("access_token", apiToken).asString
    var newMax = lastId
    if (response.code == 200) {
      parse(response.body) match {
        case arr: JArray => arr.arr foreach { elem =>
          try {
            val event = convertEvent(elem.extract[EventParse])
            newMax = math.max(newMax, event.id)
            if (!first && event.id > lastId) {
              receiver.store(event)
            }
          } catch {
            case ex: Exception => println(s"ERROR parsing user status: $elem\n$ex")
          }
        }
        case other => logError(s"could not process $other")
      }
      lastId = newMax
    } else {
      println(s"ERROR getting user: $username's status:\n$response")
    }
  }

  def run(): Unit = {

    Thread.sleep(Random.nextInt(pollTime.toInt)) // do not have all wake up at the same time
    fetchStatus(true)
    while (true) {
      try {
        Thread.sleep(pollTime)
        fetchStatus()
      } catch {
        case ex: InterruptedException => logError(s"interrupt exception: $ex")
      }
    }
  }
}