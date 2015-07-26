package edu.rit.csh.wikiData

import Numeric.Implicits._

object Utils {

  /**
   * Splits the list on where the conditional returns false
   * @param lst List[T]
   * @param cond T => Boolean
   * @return a tuple where the first element is the number of elements before and the
   *         second element is the rest of the list
   */
  def splitOn[T](lst: List[T], cond: T => Boolean): (Int, List[T]) = lst match {
    case Nil => (0, Nil)
    case lst @ x :: xs => if (cond(x)) {
      val (before, after) = splitOn(xs, cond)
      (1 + before, after)
    } else {
      (0, lst)
    }
  }

  /** Generates the weighted moving average on the list of data points */
  def movingWeightedAverage[T](data: List[T], size: Int)(implicit num: Numeric[T]): Double = {
    def numerator(data: List[T], size: Int): Double = data match {
      case head :: tail => head.toDouble * size + numerator(tail, size - 1)
      case Nil => 0
    }
    numerator(data, size) / ((size * (size + 1)) / 2.0)
  }

  def mean[T](data: Seq[T])(implicit num: Numeric[T]): Double = {
    val (sum, len) = data.foldLeft((num.zero, 0)) { case ((sum, len), n) => (sum + n, len + 1) }
    if (len == 0) 0.0
    else sum.toDouble / len.toDouble
  }

  def stdDev[T](xs: List[T], avg: Double)(implicit num: Numeric[T]): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) { (a,e) => a + math.pow(e.toDouble - avg, 2.0) } / xs.size)
  }

  def groupPoints[T](data: List[T], currentPoint: T, endPoint: T, duration: T)(implicit num: Numeric[T]): List[(T, Int)] = data match {
    case Nil =>
      if (num.gt(currentPoint, endPoint)) {
        (currentPoint, 0) :: groupPoints(Nil, currentPoint - duration, endPoint, duration)
      } else {
        Nil
      }
    case lst =>
      def comp(pt: T): Boolean = num.gt(pt, currentPoint - duration)
      val (before, after) = splitOn(data, comp)
      (currentPoint, before) :: groupPoints(after, currentPoint - duration, endPoint, duration)
  }
}
