package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.DBScanCube
import org.apache.spark.Scala.utils.partition.Cell_3D.getCube
import org.apache.spark.Scala.utils.partition.CellGraph_3D.getcellGraph
import scala.util.control.Breaks._
import scala.collection.{breakOut, mutable}

object CubeSplitPartition{
  def getPartition(pointCube:Set[(DBScanCube, Int)], bounding: Double,maxPointsPerPartition:Int): List[(DBScanCube, Int)] = {
    new CubeSplitPartition(pointCube, bounding,maxPointsPerPartition).getSplits()
  }
}


case class CubeSplitPartition(pointCube:Set[(DBScanCube, Int)], bounding: Double,maxPointsPerPartition:Int) {
  def getSplits(): List[(DBScanCube, Int)] = {
    val pointofCube: Set[(Int, DBScanCube, Int)] = getCube(pointCube, bounding)
    val cellgraph: Graph = getcellGraph(pointofCube, bounding)
    println("About to start partitioning...")
    val partitions = partition(cellgraph, pointofCube)
    println("the Partitions are below:")
    partitions.foreach(println)
    println("Partitioning Done")
    partitions.filter({
      case (_, count) => count > 0
    })
  }

  def combine(cubes: Set[DBScanCube]): DBScanCube ={

  }

  def getpartions(cubepartition: List[Set[Int]],pointofCube: Set[(Int, DBScanCube, Int)]): List[(DBScanCube, Int)] ={
    //根据索引将Cube合并，并将其中的点相加
    var partitions:List[(DBScanCube, Int)]=List[(DBScanCube, Int)]()
    for(part <- cubepartition){
      var sum:Int = 0
      var cubesum:Set[DBScanCube]=Set()
      for(index <- part){
        pointofCube.find { case (idx, cube, count) => idx == index } match {
          case Some((_, cube, count)) =>
            sum += count
            cubesum += cube
        }
      }
      partitions = (combine(cubesum),sum) :: partitions
    }
    partitions
  }

  def partition(cellgraph: Graph, pointofCube: Set[(Int, DBScanCube, Int)]):List[(DBScanCube, Int)] = {
    var cubepartition: List[Set[Int]] = List() //每个分区的Cube索引
    var visited: Set[Int] = Set()
    for (vertex <- cellgraph.vertices) { //遍历分区，让同一分区的Cube索引存在一起
      if(!visited.contains(vertex)){
        visited += vertex
        var cubelist:Set[Int]=Set()
        cubelist += vertex

        // 找出与当前顶点相连的所有边
        val connectedEdges = cellgraph.edges.collect {
          case ((v1, v2), weight) if v1 == vertex && !visited.contains(v2) => ((v1, v2), weight)
        }.toList

        // 权重从小到大排序
        val sortedEdges = connectedEdges.sortBy { case (_, weight) => weight }

        var sum:Int = 0
        pointofCube.find { case (idx, cube, count) => idx == vertex } match {
          case Some((_, _, count)) => sum += count
        }

        for(((_, v2), _) <- sortedEdges){
          if(sum<maxPointsPerPartition){
            pointofCube.find { case (idx, cube, count) => idx == v2 } match {
              case Some((_, _, count)) => sum += count
            }
            cubelist += v2
          }
          else break
        }
        cubepartition = cubelist :: cubepartition
      }
    }
    getpartions(cubepartition,pointofCube)
  }

}