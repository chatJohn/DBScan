package org.apache.spark.Scala.utils.partition
import org.apache.spark.Scala.DBScan3DNaive.DBScanCube

import scala.collection.mutable

object Kernighan_Lin{
  def getPartition(pointofCube:Set[(Int, DBScanCube, Int)], cellgraph:Graph, PointsPerPartition:Int): List[Set[DBScanCube]] = {
    new Kernighan_Lin(pointofCube,cellgraph,PointsPerPartition).KLresult()
  }
}

case class Kernighan_Lin(pointofCube:Set[(Int, DBScanCube, Int)],cellgraph: Graph, PointsPerPartition:Int) {

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

  def reduction(internal: Set[Int], external: Set[Int], node: Int): Double = {
    sumWeights(external, node) - sumWeights(internal, node)
  }

  def computeD(A: Set[Int], B: Set[Int]): Map[Int, Double] = {
    val D = mutable.Map[Int, Double]()
    for (i <- A) {
      D(i) = reduction(A, B, i)
    }
    for (i <- B) {
      D(i) = reduction(B, A, i)
    }
    D.toMap
  }

  def maxSwitchCostNodes(A: Set[Int], B: Set[Int], D: Map[Int, Double]): (Int, Int, Double) = {
    var maxCost = Double.MinValue
    var a = 0
    var b = 0
    for (i <- A; j <- B) {
      val cost = D(i) + D(j) - 2 * getWeight(i, j)
      if (cost > maxCost) {
        maxCost = cost
        a = i
        b = j
      }
    }
    (a, b, maxCost)
  }

  def updateD(A: Set[Int], B: Set[Int], D: Map[Int, Double], a: Int, b: Int): Map[Int, Double] = {
    val updatedD = mutable.Map[Int, Double]()
    for (i <- A) {
      updatedD(i) = D(i) + getWeight(i, a) - getWeight(i, b)
    }
    for (i <- B) {
      updatedD(i) = D(i) + getWeight(i, b) - getWeight(i, a)
    }
    updatedD.toMap
  }

  def getMaxCostAndIndex(costs: List[Double]): (Double, Int) = {
    var maxCost = Double.MinValue
    var index = 0
    var sum = 0.0
    for ((cost, i) <- costs.zipWithIndex) {
      sum += cost
      if (sum > maxCost) {
        maxCost = sum
        index = i
      }
    }
    (maxCost, index)
  }

  def switch(A: mutable.Set[Int], B: mutable.Set[Int], k: Int): (Set[Int], Set[Int], Boolean) = {
    var D = computeD(A.toSet, B.toSet)
    var costs = List[Double]()
    var X = List[Int]()
    var Y = List[Int]()
    val len: Int = Math.max(A.size, B.size)
    //    for (_ <- 1 to getSize() / k + 1) {
    for (_ <- 1 to len) {
      val (x, y, cost) = maxSwitchCostNodes(A.toSet, B.toSet, D)
      if (x != 0 && y != 0) {
        A.remove(x)
        B.remove(y)
        costs :+= cost
        X :+= x
        Y :+= y
        D = updateD(A.toSet, B.toSet, D, x, y)
      } else if (B.nonEmpty) {
        Y :+= B.head
        B.remove(B.head)
      } else if (A.nonEmpty) {
        X :+= A.head
        A.remove(A.head)
      }
    }

    val (maxCost, index) = getMaxCostAndIndex(costs)

    if (maxCost > 0) {
      val newA = (Y.take(index + 1) ++ X.drop(index + 1)).toSet
      val newB = (X.take(index + 1) ++ Y.drop(index + 1)).toSet
      (newA, newB, false)
    } else {
      (X.toSet, Y.toSet, true)
    }
  }

  def print_partion_weight(partitions:mutable.Map[Int, mutable.Set[Int]]): Unit ={
    var sumAll = 0.0
    for (i <- 0 until partitions.size) {
      print(s"\nPartition $i : ")
      var sum = 0.0
      for (node <- partitions(i)) {
        print(node + " ")
        sum += sumWeights(partitions(i).toSet, node)
      }
      println(s"internal weight part $sum")
      sumAll += sum
    }
    println(s"internal weight ${ sumAll }")
    println(s"external weight ${weightsum - sumAll}")
  }

  def points_in_partition(partition:mutable.Set[Int]): Int ={
    var sum = 0
    for (node <- partition) {
      pointofCube.find { case (idx, cube, count) => idx == node } match {
        case Some((_, _, count)) =>
          sum += count
      }
    }
    sum
  }

  def split_merge(partitions_points:mutable.Map[Int, (mutable.Set[Int],Int)],
                  maxPointsPerPartition:Int,minPointsPerPartition:Int): mutable.Map[Int, mutable.Set[Int]] ={
    val new_partitions = mutable.Map[Int,mutable.Set[Int]]()
    var tomergePartition = mutable.Set[Int]()
    var tomergePoints = 0
    var index = 0
    for ((_, (nodes, points)) <- partitions_points){
      // 在点数限制范围内，直接加入
      if(points < maxPointsPerPartition && points > minPointsPerPartition){
        new_partitions(index) = nodes
        index = index + 1
      }
      //点数大于上限，进行拆分
      else if(points >= maxPointsPerPartition){
        val numSplits = Math.ceil(points.toDouble / maxPointsPerPartition).toInt
        val idealSize = Math.ceil(nodes.size.toDouble / numSplits).toInt
        val splitNodes = nodes.grouped(idealSize).toList
        for (splitPartition <- splitNodes) {
          new_partitions(index) = splitPartition
          index = index + 1
        }
      }
      //点数小于下限的分区进行合并
      else if(points <= minPointsPerPartition){
        if (tomergePoints + points < minPointsPerPartition) {
          tomergePartition ++= nodes
          tomergePoints += points
        }
        else {
          val mergedPartition = mutable.Set[Int]() ++ tomergePartition
          mergedPartition ++= nodes
          new_partitions(index) = mergedPartition
          index += 1
          tomergePartition.clear()
          tomergePoints = 0
        }
      }
    }
    if(tomergePartition.nonEmpty) new_partitions(index) = tomergePartition
    new_partitions
  }


  def KLresult(): List[Set[DBScanCube]] = {
    // 初始化分区
    val partitions = mutable.Map[Int, mutable.Set[Int]]()
    val k = PointsPerPartition
    for (i <- 0 until k) {
      partitions(i) = mutable.Set[Int]()
    }
    println("In partition 1....")
    // 随机初始化
    var partitionIndex = 0
    for (i <- 1 to getSize) {
      partitions(partitionIndex % k) += i
      partitionIndex += 1
    }

    for (i <- partitions.keys; j <- partitions.keys if i < j) {
      var done = false
      while (!done) {
        val (newA, newB, isDone) = switch(partitions(i), partitions(j), k)
        partitions(i) = newA.to[mutable.Set]
        partitions(j) = newB.to[mutable.Set]
        done = isDone
      }
    }
    println("In partition 2....")

    // 返回最终分区结果
    var cubepartition: List[Set[DBScanCube]] = List()
    var cubelist:Set[DBScanCube]=Set()
    var sum = 0
    var summax:Int = 0
    var summin:Int = Int.MaxValue
    for (i <- 0 until partitions.size) {  //new_partition
      for (node <- partitions(i)) {  //new_partition
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