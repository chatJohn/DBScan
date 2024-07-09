package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.DBScanCube

import scala.collection.mutable
object CostBasedPartition {
  def partition(toSplit: Set[(DBScanCube, Int)],
                cubeSize: Int,
                maxCost: Int): List[(DBScanCube, Int)] = {
    new CostBasedPartition(cubeSize).CBP(toSplit, maxCost)
  }
}

class CostBasedPartition(cubeSize: Double) {
  type CubeWithCount = (DBScanCube, Int)

  def estimateCost(space: Set[CubeWithCount], cube: DBScanCube): Int = {
    val cost: Int = space.view
      .filter({
        case (current, _) => cube.contains(current)
      })
      .foldLeft(0)({
        case (total, (_, currentCubeCount)) => total + currentCubeCount
      })
    cost
  }
  private def findPossibleSplit(cube: DBScanCube): Set[DBScanCube] = {
    val splitX = (cube.x + cubeSize) until cube.x2 by cubeSize
    val splitY = (cube.y + cubeSize) until cube.y2 by cubeSize
    val splitT = (cube.t + cubeSize) until cube.t2 by cubeSize
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
          DBScanCube(box.x2, box.y, box.t, boundary.x2, boundary.y2, boundary.t2)
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
  def split(cube: DBScanCube, cost: (DBScanCube) => Int): (DBScanCube, DBScanCube) = {
    var costDiff: Int = Int.MaxValue
    var finalCube = (cube, cube)
    findPossibleSplit(cube).foreach(x => {
      val currentCostDiff = math.abs(cost(x) - cost(complement(x, cube)))
      if(currentCostDiff < costDiff){
        costDiff = currentCostDiff
        finalCube = (x, complement(x, cube))
      }
    })
   finalCube
  }
  def findBoundingCube(cubeWithCount: Set[CubeWithCount]): DBScanCube = {
    val invertedCub = DBScanCube(Double.MaxValue, Double.MaxValue, Double.MaxValue,
      Double.MinValue, Double.MinValue, Double.MinValue) // build the initial cube

    val boundingCube: DBScanCube = cubeWithCount.foldLeft(invertedCub) {
      case (bounding, (c, _)) => DBScanCube(bounding.x.min(c.x), bounding.y.min(c.y), bounding.t.min(c.t),
        bounding.x2.max(c.x2), bounding.y2.max(c.y2), bounding.t2.max(c.t2))
    }
    boundingCube
  }
  def CBP(toSplit: Set[(DBScanCube, Int)], maxCost: Int): List[CubeWithCount] = {
    val boundingCube: DBScanCube = findBoundingCube(toSplit)
    def pointsIn: DBScanCube => Int = estimateCost(toSplit, _: DBScanCube)
    val taskQueue = mutable.Queue(boundingCube)
    var partitions: List[(DBScanCube, Int)] = List[CubeWithCount]()
    while(taskQueue.nonEmpty){
      val cube: DBScanCube = taskQueue.dequeue()
      val cost = estimateCost(toSplit, cube)
      if(cost > maxCost){
        val tuple = split(cube, pointsIn)
        val s1 = tuple._1
        val s2 = tuple._2
        taskQueue.enqueue(s1)
        taskQueue.enqueue(s2)
      }else{
        partitions = (cube, cost)::partitions
      }
    }
    partitions
  }
}
