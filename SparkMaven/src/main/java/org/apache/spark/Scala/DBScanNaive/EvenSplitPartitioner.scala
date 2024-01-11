package org.apache.spark.Scala.DBScanNaive

import org.apache.spark.internal.Logging

import scala.annotation.tailrec

/*
* Helper method for calling the partitioner
* */

object EvenSplitPartitioner {
  def partition(toSplit: Set[(DBSCANCuboid, Int)],
                maxPointsPerPartition: Long,
                minimunRectangleSize: Double,
                minimunHigh: Double): List[(DBSCANCuboid, Int)] ={
    new EvenSplitPartitioner(maxPointsPerPartition, minimunRectangleSize,minimunHigh).findPartitions(toSplit)
  }
}

/**
 *
 * @param maxPointsPerPartition 每个分区中的最多的点数
 * @param minimumRectangleSize 最小的长方体长宽
 * @param minimunHigh 最小的长方体高
 */
class EvenSplitPartitioner(maxPointsPerPartition:Long, minimumRectangleSize: Double,minimunHigh: Double) extends Logging{
  type RectangleWithCount = (DBSCANCuboid, Int)

  /*
  * Return the Rectangle which bounding the all point in partition
  * */
  def findBoundingRectangle(rectangleWithCount: Set[RectangleWithCount]): DBSCANCuboid = {
    val invertedRectangle =
      DBSCANCuboid(Double.MaxValue, Double.MaxValue,Double.MaxValue,
        Double.MinValue, Double.MinValue, Double.MinValue)// build the initial Cuboid

    rectangleWithCount.foldLeft(invertedRectangle){
      case (bounding, (c, _)) => DBSCANCuboid(bounding.x.min(c.x), bounding.y.min(c.y), bounding.t.min(c.t),
        bounding.x2.max(c.x2), bounding.y2.max(c.y2), bounding.t2.max(c.t2))
    }
  }


  /*
  * Return the number of points in the rectangle
  * */
  def pointsInRectangle(space: Set[RectangleWithCount], rectangle: DBSCANCuboid): Int = {
    space.view
      .filter({
        case (current, _) => rectangle.contains(current)
      })
      .foldLeft(0)({
        case (total, (_, currentRecCount)) => total + currentRecCount
      })
  }


  /**
   * Return true if the given rectangle can be split into at least two rectangles of minimum size, and must be sure to develop inner rectangle
   * @param box
   * @return tow split box which can make up to box
   */
  //*****再考虑下逻辑******
  private def canBeSplit(box: DBSCANCuboid): Boolean ={
    box.x2 - box.x > minimumRectangleSize * 2 ||
    box.y2 - box.y > minimumRectangleSize * 2 ||
    box.t2 - box.t > minimunHigh * 2
  }


  /*
  * Return the all possible split ways in which the given box can be split
  * */
  private def findPossibleSplit(box: DBSCANCuboid): Set[DBSCANCuboid] ={
    val splitX = (box.x + minimumRectangleSize) until box.x2 by minimumRectangleSize
    val splitY = (box.y + minimumRectangleSize) until box.y2 by minimumRectangleSize
    val splitT = (box.t + minimunHigh) until box.t2 by minimunHigh
    val splitRectangles = splitX.map(x => DBSCANCuboid(box.x, box.y, box.t, x, box.y2, box.t2)) ++
      splitY.map(y => DBSCANCuboid(box.x, box.y, box.t, box.x2, y, box.t2))++
      splitT.map(t => DBSCANCuboid(box.x, box.y, box.t, box.x2, box.y2, t))
    println(s"Possible splits: $splitRectangles")
    splitRectangles.toSet
  }


  /*
  * Return the box that covers the space inside boundary which is not covered by the box
  *
  * if the box is valid for boundary, return another rectangle which this one combine the box is boundary
  *
  * */
  private def complement(box: DBSCANCuboid, boundary: DBSCANCuboid): DBSCANCuboid = {
    if(box.x == boundary.x && box.y == boundary.y && box.t == boundary.t){
      if(boundary.x2 >= box.x2 && boundary.y2 >= box.y2 && boundary.t2 >= box.t2){
        if(box.y2 == boundary.y2){
          DBSCANCuboid(box.x2, box.y, box.t, boundary.x2, boundary.y2, boundary.t2) //
        }else if(box.x2 == boundary.x2){
          DBSCANCuboid(box.x, box.y, box.t2, boundary.x2, boundary.y2, boundary.t2)
        }
        else if(box.t2 == boundary.t2){
          DBSCANCuboid(box.x, box.y2, box.t, boundary.x2, boundary.y2, boundary.t2)
        }
        else{
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
  def split(rectangle: DBSCANCuboid, cost: (DBSCANCuboid) => Int):
  (DBSCANCuboid, DBSCANCuboid) = {
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
    (smallestSplit, complement(smallestSplit, rectangle))
  }


  @tailrec
  private def partition(remaining: List[RectangleWithCount], partitioned: List[RectangleWithCount],
                        pointsIn: (DBSCANCuboid) => Int): List[RectangleWithCount] = {

    remaining match {
      case (rectangle, count) :: rest =>
        if (count > maxPointsPerPartition) {
          if (canBeSplit(rectangle)) {
            println(s"About to split $rectangle")

            def cost = (rec: DBSCANCuboid) => ((pointsIn(rectangle) / 2) - pointsIn(rec)).abs

            val (split1, split2) = split(rectangle, cost)
            println(s"Find the splits: $split1, $split2")
            val s1 = (split1, pointsIn(split1))
            val s2 = (split2, pointsIn(split2))
            partition(s1 :: s2 :: rest, partitioned, pointsIn)
          } else {
            println(s"Can't split: ($rectangle -> $count)," +
              s" maxPointsSize: $maxPointsPerPartition")
            partition(rest, (rectangle, count) :: partitioned, pointsIn)// change point
          }
        } else {
          partition(rest, (rectangle, count) :: partitioned, pointsIn)
        }
      case Nil => partitioned
    }
  }

  def findPartitions(toSplit: Set[RectangleWithCount]): List[RectangleWithCount] = {
    val boundingRectangle: DBSCANCuboid = findBoundingRectangle(toSplit)
    def pointsIn = pointsInRectangle(toSplit, _: DBSCANCuboid)
    val toPartition = List((boundingRectangle, pointsIn(boundingRectangle)))
    val partitioned = List[RectangleWithCount]()
    println("About to start partitioning...")
    val partitions: List[(DBSCANCuboid, Int)] = partition(toPartition, partitioned, pointsIn)
    println("the Partitions are below:")
    partitions.foreach(println)
    println("Partitioning Done")

    partitions.filter({
      case (_, count) => count > 0
    })
  }
}