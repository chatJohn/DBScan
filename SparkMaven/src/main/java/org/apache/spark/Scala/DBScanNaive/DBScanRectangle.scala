package org.apache.spark.Scala.DBScanNaive

import org.apache.spark.Scala.DBScanNaive.DBScanPoint

/*
* A rectangle with left down corner of (x, y) and right upper corner of (x2, y2)
* */
case class DBScanRectangle(x: Double, y: Double, x2: Double, y2: Double) {
  /*
  * Return whether other rectangle is contained by this one
  * */
  def contains(other: DBScanRectangle): Boolean = {
    x <= other.x && other.x2 <= x2 && y <= other.y && other.y2 <= y2
  }

  /*
  * Return whether the point is contained by the rectangle
  * */
  def contains(point: DBScanPoint): Boolean = {
    x <= point.x && point.x <= x2 && y <= point.y && point.y <= y2
  }

  /*
  * Return the new DBScanRectangle from shrinking this rectangle by given amount
  * if the mount is positive, the rectangle becomes the inner rectangle, otherwise, it becomes the outer rectangle
  * */
  def shrink(amount: Double): DBScanRectangle = {
    DBScanRectangle(x + amount, y + amount, x2 - amount, y2 - amount)
  }


  /*
  * Return whether the point is contained by the rectangle, and the point is not in the border of rectangle
  * that means the point is totally in the rectangle
  * */
  def almostContains(point: DBScanPoint): Boolean = {
    x < point.x && point.x < x2 && y < point.y && point.y < y2
  }

}
