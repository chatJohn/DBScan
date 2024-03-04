package org.apache.spark.Scala.DBScan3DNaive


import org.apache.spark.internal.Logging

import scala.annotation.tailrec
object EvenSplitPartition_3D {
  def partition(toSplit: Set[(DBScanCube, Int)],
                maxPointPerPartiton: Long,
                minimumRectangleSize: Double,
                minimumHigh: Double,
                distanceEps:Double,
                timeEps:Double): List[(DBScanCube, Int)] = {
    new EvenSplitPartition_3D(maxPointPerPartiton, minimumRectangleSize, minimumHigh,distanceEps,timeEps).findPatitions(toSplit)
  }
}


class EvenSplitPartition_3D(maxPointsPerPartition: Long, minimumRectangleSize: Double, minimumHigh: Double,distanceEps:Double,timeEps:Double) extends Logging{
  type CubeWithCount = (DBScanCube, Int)


  /**
   * Return the Cube which bounding the all points in partitions
   * @param cubeWithCount
   * @return
   */
  def findBoundingCube(cubeWithCount: Set[CubeWithCount]): DBScanCube = {
    val invertedCub = DBScanCube(Double.MaxValue, Double.MaxValue, Double.MaxValue,
      Double.MinValue, Double.MinValue, Double.MinValue) // build the initial cube

    val boundingCube: DBScanCube = cubeWithCount.foldLeft(invertedCub) {
      case (bounding, (c, _)) => DBScanCube(bounding.x.min(c.x), bounding.y.min(c.y), bounding.t.min(c.t),
        bounding.x2.max(c.x2), bounding.y2.max(c.y2), bounding.t2.max(c.t2))
    }
    boundingCube
  }

  /**
   * Return the number of points in the Cube
   * @param space
   * @param cube
   * @return
   */
  def pointsInCube(space: Set[CubeWithCount], cube: DBScanCube): Int = {
    val count: Int = space.view
      .filter({
        case (current, _) => cube.contains(current)
      })
      .foldLeft(0)({
        case (total, (_, currentCubeCount)) => total + currentCubeCount
      })
    count
  }

  /**
   * three dimension can be split
   * @param cube
   * @return
   */
  private def canBeSplit(cube: DBScanCube): Boolean = {
    cube.x2 - cube.x > minimumRectangleSize * 2 ||
      cube.y2 - cube.y > minimumRectangleSize * 2 ||
      cube.t2 - cube.t > minimumHigh * 2
  }

  /**
   * Return the all possible split ways in which the given box can be split
   * @param cube
   * @return
   */
  private def findPossibleSplit(cube: DBScanCube): Set[DBScanCube] = {
    val splitX = (cube.x + minimumRectangleSize) until cube.x2 by minimumRectangleSize
    val splitY = (cube.y + minimumRectangleSize) until cube.y2 by minimumRectangleSize
    val splitT = (cube.t + minimumHigh) until cube.t2 by minimumHigh
    val splitCubes = splitX.map(x => DBScanCube(cube.x, cube.y, cube.t, x, cube.y2, cube.t2)) ++
      splitY.map(y => DBScanCube(cube.x, cube.y, cube.t, cube.x2, y, cube.t2))++
      splitT.map(t => DBScanCube(cube.x, cube.y, cube.t, cube.x2, cube.y2, t))
    println(s"Possible splits: $splitCubes")
    splitCubes.toSet
  }
  private def complement(box: DBScanCube, boundary: DBScanCube): DBScanCube = {
    if(box.x == boundary.x && box.y == boundary.y && box.t == boundary.t){
      if(boundary.x2 >= box.x2 && boundary.y2 >= box.y2 && boundary.t2 >= box.t2){
        if(box.y2 == boundary.y2&&box.t2 == boundary.t2){
          DBScanCube(box.x2, box.y, box.t, boundary.x2, boundary.y2, boundary.t2) //
        }else if(box.x2 == boundary.x2&&box.y2 == boundary.y2){
          DBScanCube(box.x, box.y, box.t2, boundary.x2, boundary.y2, boundary.t2)
        }
        else if(box.x2 == boundary.x2&&box.t2 == boundary.t2){
          DBScanCube(box.x, box.y2, box.t, boundary.x2, boundary.y2, boundary.t2)
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

  /**
   * find the smallest cost split
//   * @param cube
//   * @param cost
//   * @return
   */
  def split(cube: DBScanCube, cost: (DBScanCube) => Int): (DBScanCube, DBScanCube) = {
    val smallestSplit = findPossibleSplit(cube).reduceLeft({
      (smallest, current) => {
        if (cost(smallest) <= cost(current)) {
          smallest
        } else {
          current
        }
      }
    })
    (smallestSplit, complement(smallestSplit, cube))
  }


  @tailrec
  private def partition(remaining: List[CubeWithCount], partitioned: List[CubeWithCount],
                        pointsIn: DBScanCube => Int): List[CubeWithCount] = {
    remaining match {
      case (cube, count):: rest =>
        if(count > maxPointsPerPartition){
          if(canBeSplit(cube)){
            println(s"About to split $cube")

            def cost1: DBScanCube => Int = (rec: DBScanCube) => ((pointsIn(cube) / 2) - pointsIn(rec)).abs
            def cost2(rectangle1: DBScanCube):Int={
              val points1 = pointsIn(rectangle1.shrink(distanceEps,timeEps))
              val points2 = pointsIn(rectangle1)
              val rectangle2: DBScanCube = complement(rectangle1, cube)
              val points3 = pointsIn(rectangle2.shrink(distanceEps,timeEps))
              val points4 = pointsIn(rectangle2)
              points2 - points1 + points4 - points3
            }
            val (split1, split2) = split(cube, cost2)
            println(s"Find the splits: $split1, $split2")
            val s1 = (split1, pointsIn(split1))
            val s2 = (split2, pointsIn(split2))
            partition(s1 :: s2 :: rest, partitioned, pointsIn)
          }else{
            println(s"Can't split: ($cube -> $count)," +
              s" maxPointsSize: $maxPointsPerPartition")
            partition(rest, (cube, count) :: partitioned, pointsIn)// change point
          }
        }else{
          partition(rest, (cube, count) :: partitioned, pointsIn)
        }
      case Nil => partitioned
    }
  }

  def findPatitions(toSplit: Set[CubeWithCount]): List[CubeWithCount] = {
    val boundingCube: DBScanCube = findBoundingCube(toSplit)
    def pointsIn: DBScanCube => Int = pointsInCube(toSplit, _: DBScanCube)
    val toPartition=List((boundingCube, pointsIn(boundingCube)))
    val patitioned=List[CubeWithCount]()
    println("About to start partitioning...")
    val partitions = partition(toPartition, patitioned, pointsIn)
    println("the Partitions are below:")
    partitions.foreach(println)
    println("Partitioning Done")
    partitions.filter({
      case (_, count) => count > 0
    })
  }
}

