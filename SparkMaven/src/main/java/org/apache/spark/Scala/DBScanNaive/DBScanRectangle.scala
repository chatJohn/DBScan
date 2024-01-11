package org.apache.spark.Scala.DBScanNaive

import org.apache.spark.Scala.DBScanNaive.DBScanPoint

/*
* A cuboid with left down corner of (x, y, t) and right upper corner of (x2, y2 ,t2)
* */
case class DBSCANCuboid(x: Double, y: Double, t: Double, x2: Double, y2: Double, t2: Double) {
  /*
  * Return whether other rectangle is contained by this one
  * */
  def contains(other: DBSCANCuboid): Boolean = {
    x <= other.x && other.x2 <= x2 && y <= other.y && other.y2 <= y2 && t <= other.t && other.t2 <= t2
  }

  /*
  * Return whether the point is contained by the rectangle
  * */
  def contains(point: DBScanPoint): Boolean = {
    x <= point.x && point.x <= x2 && y <= point.y && point.y <= y2 && t <= point.t && point.t <= t2
  }

  /*
  * Return the new DBSCANCuboid from shrinking this rectangle by given amount
  * if the mount is positive, the rectangle becomes the inner rectangle, otherwise, it becomes the outer rectangle
  * */
  def shrink(amount: Double,amount2: Double): DBSCANCuboid = {
    DBSCANCuboid(x + amount, y + amount, t + amount2, x2 - amount, y2 - amount, t2 - amount2)
  }


  /*
  * Return whether the point is contained by the rectangle, and the point is not in the border of rectangle
  * that means the point is totally in the rectangle
  * */
  def almostContains(point: DBScanPoint): Boolean = {
    x < point.x && point.x < x2 && y < point.y && point.y < y2 && t < point.t && point.t < t2
  }

}
