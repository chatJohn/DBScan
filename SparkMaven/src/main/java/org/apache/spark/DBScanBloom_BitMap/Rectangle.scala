package org.apache.spark.DBScanBloom_BitMap

import org.apache.spark.DBScanBloom_BitMap.Point

case class Rectangle(leftDownX: Double, leftDownY: Double, rightUpX: Double, rightUpY: Double){
  /*
  * Return whether other rectangle is contained by this one
  * */
  def contains(other: Rectangle): Boolean ={
    leftDownX <= other.leftDownX && other.rightUpX <= rightUpX && leftDownY <= other.leftDownY  && other.rightUpY <= rightUpY
  }

  /*
  * Return whether the point is contained by the rectangle
  * */
  def contains(point: Point): Boolean={
    leftDownX <= point.x && point.x <= rightUpX && leftDownY <= point.y && point.y <= rightUpY
  }

  /*
  * Return the new DBScanRectangle from shrinking this rectangle by given amount
  * if the mount is positive, the rectangle becomes the inner rectangle, otherwise, it becomes the outer rectangle
  * */
  def shrink(amount: Double): Rectangle ={
    Rectangle(leftDownX + amount, leftDownY + amount, rightUpX - amount, rightUpY - amount)
  }


  /*
  * Return whether the point is contained by the rectangle, and the point is not in the border of rectangle
  * that means the point is totally in the rectangle
  * */
  def almostContains(point: Point): Boolean ={
    leftDownX < point.x && point.x < rightUpX && leftDownY < point.y && point.y < rightUpY
  }

}
