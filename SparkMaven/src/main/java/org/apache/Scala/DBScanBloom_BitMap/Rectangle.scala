package org.apache.spark.Scala.DBScanBloom_BitMap

case class Rectangle(leftDownX: Double, leftDownY: Double, rightUpX: Double, rightUpY: Double){

  /**
   *
   * @param other
   * @return Return whether other rectangle is contained by this one
   */
  def contains(other: Rectangle): Boolean ={
    leftDownX <= other.leftDownX && other.rightUpX <= rightUpX && leftDownY <= other.leftDownY  && other.rightUpY <= rightUpY
  }

  /**
   *
   * @param point
   * @return Return whether the point is contained by the rectangle
   */
  def contains(point: Point): Boolean={
    leftDownX <= point.x && point.x <= rightUpX && leftDownY <= point.y && point.y <= rightUpY
  }

  def hasUnit(b: Rectangle): Boolean = {
    if(this.contains(b))  true
    else {
      if(b.rightUpX > this.leftDownX  && this.leftDownY < b.rightUpY || b.rightUpX > this.leftDownX && b.rightUpY > this.leftDownY) true // left
      else if(b.rightUpX > this.leftDownX  && this.leftDownY < b.rightUpY || b.leftDownX < this.rightUpX && b.leftDownY < this.rightUpY) true // up
      else if(b.leftDownX < this.rightUpX  && this.leftDownY < b.rightUpY || b.leftDownX < this.rightUpX && b.leftDownY < this.rightUpY) true // left
      else if(b.leftDownX < this.rightUpX  && this.leftDownY < b.rightUpY || b.rightUpX > this.leftDownX && b.rightUpY > this.leftDownY) true // down
      else false
    }
  }
  /**
   * Return the new DBScanRectangle from shrinking this rectangle by given amount
   * if the mount is positive, the rectangle becomes the inner rectangle, otherwise, it becomes the outer rectangle
   * @param amount
   * @return
   */


  def shrink(amount: Double): Rectangle ={
    Rectangle(leftDownX + amount, leftDownY + amount, rightUpX - amount, rightUpY - amount)
  }

  /**
   * Return whether the point is contained by the rectangle, and the point is not in the border of rectangle,
   * that means the point is totally in the rectangle
   * @param point
   * @return
   */
  def almostContains(point: Point): Boolean ={
    leftDownX < point.x && point.x < rightUpX && leftDownY < point.y && point.y < rightUpY
  }

  /**
   * for key in BloomFilter
   * @return
   */
  override def toString: String = {
    this.leftDownX + " " + this.leftDownY + " " + this.rightUpX + " " + this.rightUpY + " "
  }
}
