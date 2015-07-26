package edu.rit.csh.github

import java.util.concurrent.Executors

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.json4s._
import org.json4s.native.JsonParser._

import scalaj.http.Http

class GithubReceiver(org: String, apiToken: String, storageLevel: StorageLevel)
    extends Receiver[Event](storageLevel) with Logging {

  private val orgMembers = s"https://api.github.com/orgs/$org/members"
  private lazy val pool = Executors.newCachedThreadPool()
  implicit lazy val formats = DefaultFormats

  private def getUserNames(str: String): Seq[String] = (parse(str) \ "login").extract[List[String]]

  private def getUserList(url: String): Seq[String] = {
    val response = Http(url).param("access_token", apiToken).asString
    if (response.code == 200) {
      val users = getUserNames(response.body)
      response.headers.get("Link") match {
        case Some(nextLink) => if (nextLink.contains(" rel=\"next\",")) {
          users ++ getUserList(nextLink.substring(1, nextLink.indexOf(">")))
        } else {
          users
        }
        case None => users
      }
    } else {
      println(s"ERROR: error updating user list:\n$response")
      Seq.empty
    }
  }

  //TODO update user being lisened to
  private val usernames: () => Seq[String] = {
    var cache: Seq[String] = Seq.empty
    val timeDiff = 60000 * 20 // update user list every 20 minutes
    var lastUpdate = System.currentTimeMillis - timeDiff

    def apply(): Seq[String] = {
      if (System.currentTimeMillis() - timeDiff >= lastUpdate) {
        lastUpdate = System.currentTimeMillis
        cache = getUserList(orgMembers)
      }
      logInfo(s"number of users = ${cache.length} : $cache")
      cache
    }
    apply
  }

  override def onStart(): Unit = {
    usernames().foreach(name => pool.submit(new UserStatusFetcher(name, apiToken, this)))
  }

  override def onStop(): Unit = {
    pool.shutdown()
  }


}
