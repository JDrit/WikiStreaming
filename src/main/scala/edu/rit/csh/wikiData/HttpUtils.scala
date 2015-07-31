package edu.rit.csh.wikiData

import org.apache.spark.Logging

import scalaj.http.Http

object HttpUtils extends Logging {
  final val WEBSERVER = "http://starfighter.csh.rit.edu:9000"

  def postLog(json: String): Unit = try {
    Http(s"$WEBSERVER/api/add_log").postData(json).asString
  } catch {
    case ex: Throwable => logError("could not update webserver's log")
  }

  def postPageEdits(json: String): Unit = try {
    Http(s"$WEBSERVER/api/add_page_edits").postData(json).asString
  } catch {
    case ex: Throwable => logError("could not update page edits")
  }

  def postActivePages(json: String): Unit = try {
    Http(s"$WEBSERVER/api/add_top_pages").postData(json).asString
  } catch {
    case ex: Throwable => logError("could not update top pages")
  }
}
