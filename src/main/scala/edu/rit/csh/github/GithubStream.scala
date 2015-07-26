package edu.rit.csh.github

import java.io.FileWriter

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object GithubStream {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("IRC Wikipedia Page Edit Stream")
      .registerKryoClasses(Array(classOf[GithubEvent]))
    val sparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sparkContext, Seconds(10))
    ssc.checkpoint("/tmp")
    val oAuthToken = ""

    val stream = ssc.receiverStream(new GithubReceiver("computersciencehouse", oAuthToken, StorageLevel.MEMORY_AND_DISK_2))

    stream.map(e => e.username).countByValueAndWindow(Minutes(24 * 60), Seconds(10)).foreachRDD { rdd =>
      val writer = new FileWriter("/users/u20/jd/public_html/github", false)
      val builder = new StringBuilder()
      builder.append("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
      builder.append("""
          |                        * Dick Measuring as a Service *
          |---------------------------------------------------------------------------------------
          |Hey CSHers, we all love measuring our dicks, so why not do it by measuring GitHub usage
          |      (most active users in the last day, this file is updated every 10 seconds)
          |---------------------------------------------------------------------------------------
          |""".stripMargin)
      rdd.takeOrdered(20)(Ordering.by[(String, Long), Long](_._2).reverse)
        .zipWithIndex
        .foreach { case ((username, count), index) =>
          builder.append("#" + String.format("%02d", Integer.valueOf(index + 1)) + " - "  +
                         String.format("%-63s", username).replace(" ", ".") +
                         String.format("%4s", count.toString).replace(" ", ".") +
                         " actions / day\n")
      }
      builder.append("\n\n\n* Don't see your name here, well you must not be important anyways")
      val output = builder.toString()
      try {
        writer.write(output)
      } finally {
        writer.flush()
        writer.close()
      }
      println(output)
    }

    ssc.start()             // Start the computation
    println("starting stream...\n\n")
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
