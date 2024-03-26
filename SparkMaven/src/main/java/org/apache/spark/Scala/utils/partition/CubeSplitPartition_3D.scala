package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.{DBScanCube, DBScanPoint_3D}
import org.apache.spark.Scala.utils.partition.Cell_3D.getCube
import org.apache.spark.Scala.utils.partition.CellGraph_3D.getcellGraph

import scala.util.control.Breaks

object CubeSplitPartition_3D{
  def getPartition(points:Array[DBScanPoint_3D], x_bounding: Double,y_bounding:
  Double,t_bounding: Double,maxPointsPerPartition:Int): List[Set[DBScanCube]] = {
    new CubeSplitPartition_3D(points,x_bounding,y_bounding,t_bounding,maxPointsPerPartition).getSplits()
  }
}


case class CubeSplitPartition_3D(points:Array[DBScanPoint_3D], x_bounding: Double, y_bounding: Double, t_bounding: Double, maxPointsPerPartition:Int) {
  def getSplits(): List[Set[DBScanCube]] = {
    val pointofCube: Set[(Int, DBScanCube, Int)] = getCube(points,x_bounding,y_bounding,t_bounding)
    var sum2 = 0
    for ((id,cube,count)<- pointofCube) {
      sum2+=count
    }
    println("point in Cube",sum2)

    val cellgraph: Graph = getcellGraph(pointofCube,x_bounding,y_bounding,t_bounding)
    println("vertices",cellgraph.vertices.size,"edges",cellgraph.edges.size)

    println("About to start partitioning...")
    val partitions = partition(cellgraph, pointofCube)
    println("the Partitions are below:")
//    partitions.foreach(println)
    println(partitions.size)
    println("Partitioning Done")
    partitions
  }

  def partition(cellgraph: Graph, pointofCube: Set[(Int, DBScanCube, Int)]):List[Set[DBScanCube]] = {
    var cubepartition: List[Set[DBScanCube]] = List() //每个分区由若干个Cube组成
    var visited: Set[Int] = Set()
    for (vertex <- cellgraph.vertices) { //遍历分区，让同一分区的Cube索引存在一起
      if(!visited.contains(vertex)){
        visited += vertex
        var cubelist:Set[DBScanCube]=Set()  //一个分区的Cubelist
        var sum:Int = 0  //该分区内总点数，与maxPointsPerPartition比较，限定分区大小
        pointofCube.find { case (idx, cube, count) => idx == vertex } match {
          case Some((_, cube, count)) =>
            sum += count
            cubelist += cube
        }
        // 找出与当前顶点相连的所有边
        val connectedEdges = cellgraph.edges.collect {
          case ((v1, v2), weight) if v1 == vertex && !visited.contains(v2) => ((v1, v2), weight)
        }.toList

        // 权重从小到大排序
        val sortedEdges = connectedEdges.sortBy { case (_, weight) => weight }

        val loop = new Breaks
        loop.breakable {
          for (((_, v2), _) <- sortedEdges) {
            if (sum < maxPointsPerPartition) {
              pointofCube.find { case (idx, cube, count) => idx == v2 } match {
                case Some((_, cube, count)) =>
                  sum += count
                  cubelist += cube
              }
            }
            else loop.break()
          }
        }
        cubepartition = cubelist :: cubepartition
      }
    }
    cubepartition
  }
}