package org.apache.spark.Scala.utils.partition
import org.apache.spark.Scala.DBScan3DDistributed.DBScanCube

import scala.collection.mutable

object Greedy{
  def getGreedyPartition(pointofCube:Set[(Int, DBScanCube, Int)], cellgraph:Graph, PointsPerPartition:Int): List[Set[DBScanCube]] = {
    new Greedy(pointofCube,cellgraph,PointsPerPartition).KLresult()
  }
}

case class Greedy(pointofCube:Set[(Int, DBScanCube, Int)],cellgraph: Graph, PointsPerPartition:Int) {

  def getWeight(node1: Int, node2: Int): Double = {
    cellgraph.edges.getOrElse((node1, node2), 0.0)
  }

  def getSize(): Int = cellgraph.vertices.size

  def weightsum(): Double = {
    cellgraph.edges.values.sum
  }

  def sumWeights(internalSet: Set[Int], node: Int): Double = {
    var weights = 0.0
    for (i <- internalSet) {
      weights += getWeight(node, i)
    }
    weights
  }

  def idCount(cubeId: Int): Int = {
    pointofCube.find { case (idx, _, _) => idx == cubeId } match {
      case Some((_, _, count)) => count
    }
  }

  def partitionCount(partition:mutable.Set[Int]): Int ={
    var sum = 0
    for (node <- partition) {
      pointofCube.find { case (idx, cube, count) => idx == node } match {
        case Some((_, _, count)) =>
          sum += count
      }
    }
    sum
  }

  def getCost(partitions:mutable.Map[Int, mutable.Set[Int]]): Double ={
    var sumAll = 0.0
    for (i <- 0 until partitions.size) {
      var sum = 0.0
      for (node <- partitions(i)) {
        sum += sumWeights(partitions(i).toSet, node)
      }
      sumAll += sum
    }
    //internal weight - external weight 越大越好
    2*sumAll - weightsum
  }

  def deepCloneMap(map: mutable.Map[Int, mutable.Set[Int]]): mutable.Map[Int, mutable.Set[Int]] = {
    val newMap = mutable.Map[Int, mutable.Set[Int]]()
    for ((k, v) <- map) {
      newMap.put(k, v.clone())
    }
    newMap
  }

  def move(i:Int,j:Int,maxcost:Double,cube_par:mutable.Map[Int, Int],partitions:mutable.Map[Int, mutable.Set[Int]]):Boolean ={
    val partitionstemp = deepCloneMap(partitions)
    partitionstemp(cube_par(i)).remove(i)
    partitionstemp(j).add(i)
    val cost =  getCost(partitionstemp)     //计算移动后的全局权重
//    println(cost)
    if(cost>maxcost) true
    else false
  }

  def KLresult(): List[Set[DBScanCube]] = {
    // 初始化分区
    val partitions = mutable.Map[Int, mutable.Set[Int]]()
    val cube_par = mutable.Map[Int, Int]()  //存每个节点所属的分区
    val k = 12
    for (i <- 0 until k) {
      partitions(i) = mutable.Set[Int]()
    }

//    var partitionIndex = 0
//    for (i <- 1 to getSize) {
//      partitions(partitionIndex % k) += i
//      cube_par(i) = partitionIndex % k
//      partitionIndex += 1
//    }
    // 均衡初始化
    val sortedCubes = pointofCube.toSeq.sortBy(-_._3)
    val avgPointsPerPartition = sortedCubes.map(_._3).sum / k

    var pointsAdded = 0
    sortedCubes.foreach { case (cubeId, _, numPoints) =>
      val partitionIndex = pointsAdded / avgPointsPerPartition
      partitions(partitionIndex) += cubeId
      cube_par(cubeId) = partitionIndex
      pointsAdded += numPoints
    }

    println("\nBefore Greedy")
    println(getCost(partitions))
    var maxcost = getCost(partitions)
    for (i <- 1 to getSize) {
      for (j <- 0 until partitions.size) {
        if(idCount(i)+partitionCount(partitions(j))<PointsPerPartition){
          if(move(i,j, maxcost, cube_par, partitions)){//移动后cost增大,正式移动
            partitions(cube_par(i)).remove(i)
            partitions(j).add(i)
            maxcost = getCost(partitions)
            cube_par(i) = j
          }
        }
      }
    }
    println("\nAfter Greedy")
    println(getCost(partitions))

    // 返回最终分区结果
    var cubepartition: List[Set[DBScanCube]] = List()
    var cubelist:Set[DBScanCube]=Set()
    var sum = 0
    var summax:Int = 0
    var summin:Int = Int.MaxValue
    for (i <- 0 until partitions.size) {
      for (node <- partitions(i)) {
        pointofCube.find { case (idx, cube, count) => idx == node } match {
          case Some((_, cube, count)) =>
            sum += count
            cubelist += cube
        }
      }
      cubepartition = cubelist :: cubepartition
      print(sum,"")
      if(sum>summax) summax = sum
      if(sum<summin) summin = sum
      cubelist = Set()
      sum = 0
    }
    println("points in partion max-min: ",summax-summin)
    cubepartition
  }
}
