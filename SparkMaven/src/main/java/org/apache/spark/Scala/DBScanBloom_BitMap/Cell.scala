package org.apache.spark.Scala.DBScanBloom_BitMap

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
object Cell{
  def getCell(data: RDD[Vector],
              x_bounding: Double,
              y_bounding: Double,
              eps: Double): Set[Rectangle] = {
    new Cell(data, x_bounding, y_bounding, eps).getCell(data)
  }
}
case class Cell(data: RDD[Vector], x_bounding: Double, y_bounding: Double, eps: Double) {
  def minimumRectangleSize: Double = 2 * eps

  /**
   * this method can get the minimum bounding rectangle, and it can show the lower bound and upper bound
   * @param p
   * @return
   */
  def shiftIfNegative(p: Double): Double= {
    if(p < 0) {
      p - minimumRectangleSize
    } else {
      p
    }
  }
  def corner(p: Double): Double = {
    (shiftIfNegative(p) / minimumRectangleSize).intValue * minimumRectangleSize
  }

  private def getPointMinimumBoundingRectangle(vector: Vector): Rectangle = {
    val point: Point = Point(vector)
    val x = corner(point.x)
    val y = corner(point.y)
    Rectangle(x, y, x + minimumRectangleSize, y + minimumRectangleSize)
  }
  /*
   * Return the all possible split ways in which the given box can be split
   * */
  private def findPossibleCell(box: Rectangle): Set[Rectangle] ={
    val splitX = (box.leftDownX + x_bounding) until box.rightUpX by x_bounding
    val splitY = (box.leftDownY + y_bounding) until box.rightUpY by y_bounding
    val splitCells = splitX.map(x => Rectangle(box.leftDownX, box.leftDownY, x, box.rightUpY)) ++
      splitY.map(y => Rectangle(box.leftDownX, box.leftDownY, box.rightUpX, y))
    println(s"Possible splits: $splitCells")
    splitCells.toSet
  }

  def getCell(data: RDD[Vector]): Set[Rectangle] = {
      val leftXMin = {
        data.map(x =>
          getPointMinimumBoundingRectangle(x))
      }.collect().map(x => x.leftDownX).toList.min
      val leftYMin = {
        data.map(x =>
          getPointMinimumBoundingRectangle(x))
      }.collect().map(x => x.leftDownY).toList.min
      val rightXMax = {
        data.map(x =>
          getPointMinimumBoundingRectangle(x))
      }.collect().map(x => x.rightUpX).toList.max
      val rightYMax = {
        data.map(x =>
          getPointMinimumBoundingRectangle(x))
      }.collect().map(x => x.rightUpY).toList.max
      val workPlace: Rectangle = Rectangle(leftXMin, leftYMin, rightXMax, rightYMax)
      val cells = findPossibleCell(workPlace)
      cells
    }



}
