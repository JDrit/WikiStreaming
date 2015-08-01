package edu.rit.csh.wikiData

import org.apache.spark.Logging

import scalaj.http.Http

object HttpUtils extends Logging {
  final val webServer = "http://starfighter.csh.rit.edu:9000"

  private def post(extension: String)(payload: String): Unit = try {
    Http(s"$webServer/$extension").header("content-type", "application/json")
      .postData(payload).asString
  } catch {
    case t: Throwable => logError(s"could not post to $extension")
  }

  def logs: String => Unit = post("api/add_log")

  def pageEdits: String => Unit = post("api/add_page_edits")

  def activePages: String => Unit = post("api/add_top_pages")

  def activeUsers: String => Unit = post("api/add_top_users")

  def vandalism: String => Unit = post("api/add_vandalism")

  def anomalies: String => Unit = post("api/add_anomalies")

}
