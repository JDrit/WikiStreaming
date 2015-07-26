package edu.rit.csh.wikiData

import java.sql.Timestamp
import java.util.Properties

import scala.concurrent.duration._
import Numeric.Implicits._

import com.typesafe.config.ConfigFactory
import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient private var instance: Option[SQLContext] = None

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    instance match {
      case Some(inst) => inst
      case None =>
        val i = new SQLContext(sparkContext)
        instance = Some(i)
        i
    }
  }
}

case class PageEdit(channel: String, page: String, count: Long, timestamp: Timestamp)
case class UserEdit(channel: String, username: String, count: Long, timestamp: Timestamp)
case class ChannelEdit(channel: String, count: Long, timestamp: Timestamp)
case class Anomaly(channel: String, page: String, mean: Double, stdDev: Double, timestamp: Timestamp, count: Int)

object WikiStream {
  final val DAY: Long = 86400000L

  val (url, props) = {
    try {
      val conf = ConfigFactory.load()
      val props = new Properties()
      props.put("user", conf.getString("database.user"))
      props.put("password", conf.getString("database.password"))
      props.put("host", conf.getString("database.host"))
      props.put("ssl", conf.getBoolean("database.ssl").toString)
      props.put("sslmode", conf.getString("database.sslmode"))
      (conf.getString("database.url"), props)
    } catch {
      case ex: Throwable =>
        println(s"Could not read configuration file:\n$ex")
        throw ex
    }
  }

  def insertBlanks(data: List[(Long, Int)], gapSize: Long, endTime: Long): List[Int] = {
    def process(data: List[(Long, Int)], current: Option[Long]): List[Int] = (data, current) match {
      case (Nil, None) => Nil
      case (Nil, Some(last)) =>
        if (last - endTime > gapSize) 0 :: process(Nil, Some(last - gapSize))
        else Nil
      case (lst @ (time, count) :: tail, None) => count :: process(tail, Some(time))
      case (lst @ (time, count) :: tail, Some(last)) =>
        if (last - time > gapSize) 0 :: process(lst, Some(last - gapSize))
        else count :: process(tail, Some(time))
    }
    process(data, None)
  }


  /** Groups the input data by a sliding window with the duration given. This returns the first
    * element in the window along with the amount of items in the window */
  private def groupBySlidingWindow[T](data: Seq[T], duration: T)(implicit num: Numeric[T]): Seq[(T, Int)] = {
    def process(data: Seq[T], accum: Seq[(T, Int)]): Seq[(T, Int)] = data match {
      case Nil => accum
      case x :: xs => accum match {
        case Nil => process(xs, Seq((x, 1)))
        case lst @ (curTime, curCount) :: ys =>
          if (num.lteq (num.abs (x - curTime), duration) ) {
            process(xs, (curTime, curCount + 1) :: ys)
          } else {
            process(xs, (x, 1) :: lst)
          }
      }
    }
    process(data, Seq.empty).reverse
  }

  private def processStream(ssc: StreamingContext, server: String, channels: Seq[String]): Unit = {
    val sources = channels.map(channel => ssc.receiverStream(new IrcReceiver(server, channel, StorageLevel.MEMORY_ONLY)))
    val stream = ssc.union(sources)

    /** save everything to JDBC */
    stream.foreachRDD { rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      rdd.toDF().write.mode(SaveMode.Append).jdbc(url, "log", props)
    }

    /** the top most active pages per channel */
    stream.map(e => (e.channel, e.page))
      .countByValueAndWindow(Minutes(60), Minutes(1))
      .reduceByKey(_ + _)
      .map { case ((channel, page), count) => (channel, (page, count)) }
      .groupByKey()
      .flatMap { case (channel, itr: Iterable[(String, Long)]) =>
      val currentTime = new Timestamp(System.currentTimeMillis())
      itr.toList
        .sorted(Ordering.by[(java.io.Serializable, Long), Long](_._2).reverse)
        .take(20)
        .map { case (page, count) => PageEdit(channel, page, count, currentTime) }
    }.foreachRDD { rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      rdd.toDF().write.mode(SaveMode.Append).jdbc(url, "channel_top_pages", props)
    }

    /** the top most active users per channel */
    stream.map(e => (e.channel, e.username))
      .countByValueAndWindow(Minutes(60), Minutes(1))
      .reduceByKey(_ + _)
      .map { case ((channel, username), count) => (channel, (username, count)) }
      .groupByKey()
      .flatMap { case (channel, itr: Iterable[(String, Long)]) =>
      val currentTime = new Timestamp(System.currentTimeMillis())
      itr.toList
        .sorted(Ordering.by[(java.io.Serializable, Long), Long](_._2).reverse)
        .take(20)
        .map { case (username, count) => UserEdit(channel, username, count, currentTime) }
    }.foreachRDD { rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      rdd.toDF().write.mode(SaveMode.Append).jdbc(url, "channel_top_users", props)
    }

    /** vandalism detection */
    stream.filter { edit =>
      val comment = edit.comment.trim.toLowerCase
      comment.startsWith("reverting possible vandalism") || (comment.startsWith("undid revision") && comment.contains("vandalism"))
    }.foreachRDD { rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      rdd.toDF().write.mode(SaveMode.Append).jdbc(url, "vandalism", props)
    }

    /**
     * The number of edits per channel in the last hour - MOST IMPORTANT
     * This must keep its SLA since this info is display to the user in near real-time
     */
    stream.map(_.channel).countByValueAndWindow(Minutes(60), Seconds(5)).foreachRDD { rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      val currentTime = new Timestamp(System.currentTimeMillis())
      val df = rdd.map { case(channel, count) => ChannelEdit(channel, count, currentTime) }.toDF()
      df.write.mode(SaveMode.Append).jdbc(url, "channel_edit_count", props)
    }

    /** Anomaly detection for each page in the channel. */
      stream.map(e => ((e.channel, e.page), e.timestamp))
      .groupByKeyAndWindow(Minutes(60 * 24), Minutes(1))
      .flatMap { case ((channel, page), timestamps) =>
      val timeList = timestamps.toList
      if (timeList.length > 5) {
        val edits = timeList.map(_.getTime).sorted
        val groups = Utils.groupPoints(edits, System.currentTimeMillis(), System.currentTimeMillis() - 1.days.toMillis, 1.hours.toMillis)
        val formattedData = groups.map(_._2)
        val mean = Utils.movingWeightedAverage(formattedData, formattedData.length)
        val sd = Utils.stdDev(formattedData, mean)
        println(
          s"""
              |page      = $page, mean = $mean, std. dev. = $sd
              |raw edits = ${edits.mkString(", ")}
              |groups    = ${groups.mkString(", ")}
              |formated  = (${formattedData.length}) = ${formattedData.mkString(", ")}
           """.stripMargin)
        if (formattedData.count(_ == 0) < 10) {
          Some(Anomaly(channel, page, mean, sd, new Timestamp(System.currentTimeMillis()), formattedData.head))
        } else {
          None
        }
      } else {
        None
      }
    }.foreachRDD { rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      rdd.toDF().write.mode(SaveMode.Append).jdbc(url, "anomalies", props)
      println("---------------------------------------------------------")
    }
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("IRC Wikipedia Page Edit Stream")
      .registerKryoClasses(Array(classOf[Edit], classOf[PageEdit], classOf[UserEdit], classOf[ChannelEdit]))
    val sparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sparkContext, Seconds(5))
    ssc.checkpoint("/tmp")

    /*
    val channels = Seq("#en.wikisource", "#en.wikibooks", "#en.wikinews", "#en.wikiquote",
      "#en.wikipedia", "#wikidata.wikipedia", "#de.wikipedia", "#ru.wikipedia", "#nl.wikipedia",
      "#it.wikipedia", "#es.wikipedia", "#vi.wikipedia", "#ja.wikipedia", "#pt.wikipedia",
      "#zh.wikipedia", "#uk.wikipedia", "#fa.wikipedia", "#ar.wikipedia", "#fi.wikipedia",
      "#ro.wikipedia", "#hu.wikipedia", "#sr.wikipedia", "#ko.wikipedia", "#bg.wikipedia",
      "#la.wikipedia", "#el.wikipedia", "#hi.wikipedia", "#th.wikipedia", "#is.wikipedia",
      "#ga.wikipedia", "#ne.wikipedia")
    */

    //val channels = Seq("#en.wikisource", "#en.wikibooks", "#en.wikinews", "#en.wikiquote", "#en.wikipedia", "#wikidata.wikipedia")
    val channels = Seq("#en.wikipedia")

    processStream(ssc, "irc.wikimedia.org", channels)

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
