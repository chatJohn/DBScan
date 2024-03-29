package org.apache.spark.Scala.utils.partition
import org.apache.spark.Scala.DBScan3DNaive.DBScanCube
import scala.collection.mutable

object Kernighan_Lin{
  def getPartition(pointofCube:Set[(Int, DBScanCube, Int)], cellgraph:Graph, k:Int): List[Set[DBScanCube]] = {
    new Kernighan_Lin(pointofCube,cellgraph,k).KLresult()
  }
}

case class Kernighan_Lin(pointofCube:Set[(Int, DBScanCube, Int)],cellgraph: Graph, k:Int) {

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

    for (_ <- 1 to getSize() / k + 1) {
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

  def KLresult(): List[Set[DBScanCube]] = {
    val partitions = mutable.Map[Int, mutable.Set[Int]]()
    for (i <- 0 until k) {
      partitions(i) = mutable.Set[Int]()
    }

    var partitionIndex = 0
    for (i <- 1 to getSize) {
      partitions(partitionIndex % k) += i
      partitionIndex += 1
    }

    println("\nBefore KL")
    var sumAll = 0.0
    for (i <- 0 until k) {
      print(s"\nPartition $i : ")
      var sum = 0.0
      for (node <- partitions(i)) {
        print(node + " ")
        sum += sumWeights(partitions(i).toSet, node)
      }
      println(s"internal weight $sum")
      sumAll += sum
    }
    println(s"external weight ${weightsum - sumAll}")

    for (i <- partitions.keys; j <- partitions.keys if i < j) {
      var done = false
      while (!done) {
        val (newA, newB, isDone) = switch(partitions(i), partitions(j), k)
        partitions(i) = newA.to[mutable.Set]
        partitions(j) = newB.to[mutable.Set]
        done = isDone
      }
    }

    println("\nAfter KL")
    sumAll = 0
    for (i <- 0 until k) {
      print(s"\nPartition $i : ")
      var sum = 0.0
      for (node <- partitions(i)) {
        print(node + " ")
        sum += sumWeights(partitions(i).toSet, node)
      }
      println(s"internal weight $sum")
      sumAll += sum
    }
    println(s"external weight ${weightsum - sumAll}")


    var cubepartition: List[Set[DBScanCube]] = List()
    var cubelist:Set[DBScanCube]=Set()
    var sum:Int = 0
    var summax:Int = 0
    var summin:Int = 0
    for (i <- 0 until k) {
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
