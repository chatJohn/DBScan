package org.apache.spark.DBScan

import org.apache.spark.internal.Logging

import scala.annotation.tailrec

/*
* Helper method for calling the partitioner
* */

object EvenSplitPartitioner {
  def partition(toSplit: Set[(DBScanRectangle, Int)],
                maxPointsPerPartition: Long,
                minimunRectangleSize: Double): List[(DBScanRectangle, Int)] ={
    new EvenSplitPartitioner(maxPointsPerPartition, minimunRectangleSize).findPartitions(toSplit)
  }
}

/**
 *
 * @param maxPointsPerPartition 每个分区中的最多的点数
 * @param minimumRectangleSize 最小的矩形大小
 */
class EvenSplitPartitioner(maxPointsPerPartition:Long, minimumRectangleSize: Double) extends Logging{
  type RectangleWithCount = (DBScanRectangle, Int)

  /*
  * Return the Rectangle which bounding the all point in partition
  * */
  def findBoundingRectangle(rectangleWithCount: Set[RectangleWithCount]): DBScanRectangle = {
    val invertedRectangle =
      DBScanRectangle(Double.MaxValue, Double.MaxValue,
        Double.MinValue, Double.MinValue)

    rectangleWithCount.foldLeft(invertedRectangle){// build the initial rectangle
      case (bounding, (c, _)) => DBScanRectangle(bounding.x.min(c.x), bounding.y.min(c.y),
        bounding.x2.max(c.x2), bounding.y2.max(c.y2))
    }
  }


  /*
  * Return the number of points in the rectangle
  * */
  def pointsInRectangle(space: Set[RectangleWithCount], rectangle: DBScanRectangle): Int = {
    space.view
      .filter({
        case (current, _) => rectangle.contains(current)
      })
      .foldLeft(0)({
        case (total, (_, currentRecCount)) => total + currentRecCount
      })
  }



  /*
  * Return true if the given rectangle can be split into at least two rectangles of minimum size
  * */

  private def canBeSplit(box: DBScanRectangle): Boolean ={
    box.x2 - box.x > minimumRectangleSize * 2 ||
      box.y2 - box.y > minimumRectangleSize * 2
  }


  /*
  * Return the all possible split ways in which the given box can be split
  * */
  private def findPossibleSplit(box: DBScanRectangle): Set[DBScanRectangle] ={
    val splitX = (box.x + minimumRectangleSize) until box.x2 by minimumRectangleSize
    val splitY = (box.y + minimumRectangleSize) until box.y2 by minimumRectangleSize
    val splitRectangles = splitX.map(x => DBScanRectangle(box.x, box.y, x, box.y2)) ++//定y变x,步长为2*eps
      splitY.map(y => DBScanRectangle(box.x, box.y, box.x2, y)) //定x变y,步长为2*eps
    println(s"Possible splits: $splitRectangles")
    splitRectangles.toSet
  }


  /*
  * Return the box that covers the space inside boundary which is not covered by the box
  *
  * if the box is valid for boundary, return another rectangle which this one combine the box is boundary
  *
  * */
  //求划分后矩形的补矩形
  private def complement(box: DBScanRectangle, boundary: DBScanRectangle): DBScanRectangle = {
    if(box.x == boundary.x && box.y == boundary.y){
      if(boundary.x2 >= box.x2 && boundary.y2 >= box.y2){
        if(box.y2 == boundary.y2){
          DBScanRectangle(box.x2, box.y, boundary.x2, boundary.y2) //
        }else if(box.x2 == boundary.x2){
          DBScanRectangle(box.x, box.y2, boundary.x2, boundary.y2)
        }else{
          throw new IllegalArgumentException("rectangle is not a proper sub_rectangle")
        }
      }else{
        throw new IllegalArgumentException("not a suitable rectangle")
      }
    }else{
      throw new IllegalArgumentException("unequal rectangle")
    }
  }


  /*
  * Find smallest cost split
  * */
  def split(rectangle: DBScanRectangle, cost: (DBScanRectangle) => Int):
  (DBScanRectangle, DBScanRectangle) = {
    val smallestSplit = findPossibleSplit(rectangle).reduceLeft({
      (smallest, current) => {
        if (cost(smallest) <= cost(current)) {
          // In the cost function, rectangle is the brefore param in the partition
          smallest
        } else {
          current
        }
      }
    })
    (smallestSplit, complement(smallestSplit, rectangle))//计算划分的成本，并求该矩形的补矩形
  }


  @tailrec
  private def partition(remaining: List[RectangleWithCount], partitioned: List[RectangleWithCount],
                        pointsIn: (DBScanRectangle) => Int): List[RectangleWithCount] = {
    remaining match {
      case (rectangle, count) :: rest =>
        if (count > maxPointsPerPartition) {
          if (canBeSplit(rectangle)) {
            println(s"About to split $rectangle")

            def cost = (rec: DBScanRectangle) => ((pointsIn(rectangle) / 2) - pointsIn(rec)).abs

            val (split1, split2) = split(rectangle, cost)
            println(s"Find the splits: $split1, $split2")
            val s1 = (split1, pointsIn(split1))
            val s2 = (split2, pointsIn(split2))
            partition(s1 :: s2 :: rest, partitioned, pointsIn)
          } else {
            println(s"Can't split: ($rectangle -> $count)," +
              s" maxPointsSize: $maxPointsPerPartition")
            partition(rest, (rectangle, count) :: partitioned, pointsIn)  //
          }
        } else {
          partition(rest, (rectangle, count) :: partitioned, pointsIn)
        }
      case Nil => partitioned
    }
  }

  def findPartitions(toSplit: Set[RectangleWithCount]): List[RectangleWithCount] = {
    val boundingRectangle = findBoundingRectangle(toSplit) //find initicial rectangle
    def pointsIn = pointsInRectangle(toSplit, _: DBScanRectangle)
    val toPartition = List((boundingRectangle, pointsIn(boundingRectangle)))
    val partitioned = List[RectangleWithCount]()
    println("About to start partitioning...")
    val partitions = partition(toPartition, partitioned, pointsIn)
    println("Partitioning Done")

    partitions.filter({
      case (_, count) => count > 0
    })
  }
}