package edu.rit.csh.wikiData

import scala.concurrent.duration._

object Test extends App {

  val raw = "1437934218609, 1437934271988, 1437934555845, 1437934693595, 1437935713079, 1437935770840, 1437935813797, 1437935913259, 1437936502206, 1437937068004, 1437937105546, 1437937138007, 1437937279018, 1437937391795, 1437937873859, 1437937917097, 1437937946678, 1437938048068, 1437938263243"
  val edits = raw.split(",").toList.map(_.trim.toLong)

  val groups = Utils.groupPoints(edits, System.currentTimeMillis(), System.currentTimeMillis() - 1.days.toMillis, 1.hours.toMillis)

  println(edits)
  println(groups.map(_._2))
}
