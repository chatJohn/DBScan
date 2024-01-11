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
    val splitX = (box.leftDownX until box.rightUpX by x_bounding).toList :+ box.rightUpX
    val splitY = (box.leftDownY until box.rightUpY by y_bounding).toList :+ box.rightUpY
    val splitCells = for {
      x <- splitX.init
      y <- splitY.init
    } yield Rectangle(x, y, x + x_bounding, y + y_bounding)
//    println(s"Possible splits: $splitCells")
    splitCells.toSet
  }

  def bound:Double = 10 * x_bounding
//  private def findPossibleCell(box: Rectangle): Set[Rectangle] = {
//    //确定密集的部分大小
//    val leftX = (box.leftDownX + box.rightUpX) / 2 - bound
//    val rightX = (box.leftDownX + box.rightUpX) / 2 + bound
//    val leftY = (box.leftDownY + box.rightUpY) / 2 - bound
//    val rightY = (box.leftDownY + box.rightUpY) / 2 + bound
//
//    val splitX = calculateSplit(box.leftDownX, box.rightUpX, leftX, rightX, x_bounding)
//    val splitY = calculateSplit(box.leftDownY, box.rightUpY, leftY, rightY, y_bounding)
//
//    val splitCells = for {
//      x <- splitX.init
//      y <- splitY.init
//    } yield Rectangle(x, y, x + calculateBounding(x,leftX,rightX,x_bounding), y + calculateBounding(y,leftY, rightY,y_bounding))
//
//    splitCells.toSet
//  }

  private def calculateSplit(start: Double, end: Double, left: Double, right: Double,bounding: Double): List[Double] = {
    //中心部分密集，两边稀疏
    val leftSplit = (start until left by 2 * bounding).toList :+ left
    val centerSplit = (left until right by bounding).toList :+ right
    val rightSplit = (right until end by 2 * bounding).toList :+ end

    leftSplit ++ centerSplit.tail ++ rightSplit.tail
  }

  private def calculateBounding(coord: Double, left: Double, right: Double, bounding: Double): Double = {
    if (left <= coord && coord < right) {
      bounding
    }
    else if(coord<left && coord + bounding * 2>left){
      left-coord
    }
    else {
      bounding * 2
    }
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
