package edu.rit.csh.wikiData

import java.io.File
import java.sql.Timestamp

import scala.io.Source

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


case class PageEdit(channel: String, page: String, count: Long)
case class UserEdit(channel: String, username: String, count: Long)
case class Anomaly(channel: String, page: String, mean: Double, stdDev: Double, timestamp: Timestamp, count: Int)

object WikiStream {
  final val DAY: Long = 86400000L
  final val HOUR: Long = 3600000L

  private def processStream(ssc: StreamingContext, server: String, channels: List[String]): Unit = {
    val stream = ssc.receiverStream(new IrcReceiver(server, channels, StorageLevel.MEMORY_ONLY)).cache()

    /** save everything to JDBC */
    stream.foreachRDD { rdd =>
      val data = rdd.collect().map { edit =>
        ("channel" -> edit.channel) ~
          ("comment" -> edit.comment) ~
          ("diff" -> edit.diff) ~
          ("page" -> edit.page) ~
          ("timestamp" -> edit.timestamp.toString) ~
          ("username" -> edit.username)
      }.toList
      HttpUtils.postLog(compact(render(data)))
    }

    /**
     * The number of edits per channel in the last hour - MOST IMPORTANT
     * This must keep its SLA since this info is display to the user in near real-time
     */
    stream.map(_.channel).countByValueAndWindow(Minutes(60), Seconds(1)).foreachRDD { rdd =>
      val data = rdd.collect()
        .sorted(Ordering.by[(String, Long), Long](_._2).reverse)
        .map { case (channel, count) => ("channel" -> channel) ~ ("count" -> count) }
        .toList
      val json = pretty(render(data.take(10)))
      println("\n\n\n\n\n\n\n\n\n\n\n\n" + json)
      HttpUtils.postPageEdits(compact(render(("timestamp" -> System.currentTimeMillis()) ~ ("page_edits" -> data))))
    }

    /** the top most active pages per channel */
    stream.map(e => (e.channel, e.page))
      .countByValueAndWindow(Minutes(60), Seconds(10))
      .reduceByKey(_ + _)
      .map { case ((channel, page), count) => (channel, (page, count)) }
      .groupByKey()
      .flatMap { case (channel, itr: Iterable[(String, Long)]) =>
        itr.toList
          .sorted(Ordering.by[(java.io.Serializable, Long), Long](_._2).reverse)
          .take(20)
          .map { case (page, count) => PageEdit(channel, page, count) }
    }.foreachRDD { rdd =>
      val data = rdd.collect().map { pageEdit =>
        ("channel" -> pageEdit.channel) ~ ("page" -> pageEdit.page) ~ ("count" -> pageEdit.count)
      }.toList
     val json = compact(render(("timestamp" -> System.currentTimeMillis()) ~ ("top_pages" -> data)))
    }

    /** the top most active users per channel */
    stream.map(e => (e.channel, e.username))
      .countByValueAndWindow(Minutes(60), Seconds(10))
      .reduceByKey(_ + _)
      .map { case ((channel, username), count) => (channel, (username, count)) }
      .groupByKey()
      .flatMap { case (channel, itr: Iterable[(String, Long)]) =>
      itr.toList
        .sorted(Ordering.by[(java.io.Serializable, Long), Long](_._2).reverse)
        .take(20)
        .map { case (username, count) => UserEdit(channel, username, count) }
    }.foreachRDD { rdd =>
      val data = rdd.collect().map { userEdit =>
        ("channel" -> userEdit.channel) ~
          ("username" -> userEdit.username) ~
          ("count" -> userEdit.count)
      }.toList
      val json = compact(render(("timestamp" -> System.currentTimeMillis()) ~ ("top_users" -> data)))
    }

    /** vandalism detection for only english domains */
    stream.filter { edit =>
      val comment = edit.comment.trim.toLowerCase
      (edit.channel.startsWith("en.") || edit.channel == "wikidata.wikipedia") &&
        (comment.startsWith("reverting possible vandalism") ||
          (comment.startsWith("undid revision") && comment.contains("vandalism")))
    }.foreachRDD { rdd =>
      val data = rdd.collect().map { edit =>
        ("channel" -> edit.channel) ~
          ("comment" -> edit.comment) ~
          ("diff" -> edit.diff) ~
          ("page" -> edit.page) ~
          ("timestamp" -> edit.timestamp.toString) ~
          ("username" -> edit.username)
      }.toList
      val json = compact(render(data))
    }

    /** Anomaly detection for each page in the channel. */
    stream.map(e => ((e.channel, e.page), e.timestamp))
      .groupByKeyAndWindow(Minutes(60 * 24), Minutes(10))
      .flatMap { case ((channel, page), timestamps) =>
      val timeList = timestamps.toList
      val edits = timeList.map(_.getTime).sorted(Ordering[Long].reverse)
      val groups = ListUtils.groupPoints(edits, System.currentTimeMillis(), System.currentTimeMillis() - DAY, HOUR)
      val formattedData = groups.map(_._2)
      if (formattedData.count(_ != 0) > 10) {
        val mean = ListUtils.movingWeightedAverage(formattedData, formattedData.length)
        val sd = ListUtils.stdDev(formattedData, mean)
        /**println(
          s"""|page      = $page, mean = $mean, std. dev. = $sd
              |raw edits = ${edits.mkString(", ")}
              |groups    = ${groups.mkString(", ")}
              |formatted  = (${formattedData.length}) = ${formattedData.mkString(", ")}""".stripMargin)*/
        Some(Anomaly(channel, page, mean, sd, new Timestamp(System.currentTimeMillis()), formattedData.head))
      } else {
        None
      }
    }.foreachRDD { rdd =>
      val data = rdd.collect().map { anomaly =>
        ("channel" -> anomaly.channel) ~
          ("page" -> anomaly.page) ~
          ("mean" -> anomaly.mean) ~
          ("standard_deviation" -> anomaly.stdDev) ~
          ("timestamp" -> anomaly.timestamp.toString) ~
          ("count" -> anomaly.count)
      }.toList
      val json = compact(render(data))
    }
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("IRC Wikipedia Page Edit Stream")
      .registerKryoClasses(Array(classOf[Edit], classOf[PageEdit], classOf[UserEdit], classOf[Anomaly]))
    val sparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sparkContext, Seconds(1))
    ssc.checkpoint("/tmp/spark-checkpoint")

    val channels = if (args.length > 0) Source.fromFile(new File(args(0))).getLines().toList
      else List("#en.wikisource", "#en.wikibooks", "#en.wikinews", "#en.wikiquote", "#en.wikipedia", "#wikidata.wikipedia")

    processStream(ssc, "irc.wikimedia.org", channels)

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
