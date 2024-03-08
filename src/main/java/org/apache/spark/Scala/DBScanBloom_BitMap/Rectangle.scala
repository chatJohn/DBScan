package org.apache.spark.Scala.DBScanBloom_BitMap

case class Rectangle(leftDownX: Double, leftDownY: Double, rightUpX: Double, rightUpY: Double){

  /**
   *
   * @param other: Rectangle
   * @return Return whether other rectangle is contained by this one
   */
  def contains(other: Rectangle): Boolean ={
    leftDownX <= other.leftDownX && other.rightUpX <= rightUpX && leftDownY <= other.leftDownY  && other.rightUpY <= rightUpY
  }

  /**
   *
   * @param point: Point
   * @return Return whether the point is contained by the rectangle
   */
  def contains(point: Point): Boolean={
    leftDownX <= point.x && point.x <= rightUpX && leftDownY <= point.y && point.y <= rightUpY
  }

  /**
   * @param b: Rectangle
   * @return
   */
  def hasUnit(b: Rectangle): Boolean = {
    if(Math.max(this.leftDownX, b.leftDownX) <= Math.min(this.rightUpX, b.rightUpX) && Math.max(this.leftDownY, b.leftDownY) <= Math.min(this.rightUpY, b.rightUpY)) true
    else false
//    !((b.rightUpX < this.leftDownX) ||  // b在this的左边
//      (b.leftDownX > this.rightUpX) ||   // b在this的右边
//      (b.rightUpY < this.leftDownY) ||   // b在this的下边
//      (b.leftDownY > this.rightUpY))      // b在this的上边
  }


  def getUnit(b: Rectangle): Rectangle = {
    val x1 = math.max(this.leftDownX, b.leftDownX)
    val y1 = math.max(this.leftDownY, b.leftDownY)
    val x2 = math.min(this.rightUpX, b.rightUpX)
    val y2 = math.min(this.rightUpY, b.rightUpY)

    if (x1 <= x2 && y1 <= y2) {
      Rectangle(x1, y1, x2, y2)
    }
    else Rectangle(0, 0, 0, 0)
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
