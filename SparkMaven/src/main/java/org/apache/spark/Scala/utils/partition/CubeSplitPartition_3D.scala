package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.{DBScanCube, DBScanPoint_3D}
import org.apache.spark.Scala.utils.partition.Cell_3D.getCube
import org.apache.spark.Scala.utils.partition.CellGraph_3D.getcellGraph
import org.apache.spark.Scala.utils.partition.Kernighan_Lin.getPartition
import scala.collection.mutable
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
//    var sum1 = 0
//    for ((id,cube,count)<- pointofCube) {
//      sum1+=count
//    }
//    println("point in Cube",sum1)

    val cellgraph: Graph = getcellGraph(pointofCube,x_bounding,y_bounding,t_bounding)
    println("cube graph vertices",cellgraph.vertices.size,"edges",cellgraph.edges.size)


    println("About to start partitioning...")
//    val partitions = partition(cellgraph, pointofCube)
//    val partitions = partition1(pointofCube)
    val partitions = getPartition(pointofCube,cellgraph,maxPointsPerPartition)

    println("the Partitions are below:")
//    partitions.foreach(println)
    println("partitions size",partitions.size)
//    var sum2 = 0
//    for(sets <- partitions){
//      sum2 += sets.size
//    }
//    print("cube in partitions",sum2)
    println("Partitioning Done")
    partitions
  }

  // a test try
  def partition1(pointofCube: Set[(Int, DBScanCube, Int)]):List[Set[DBScanCube]] = {
    var cubepartition: List[Set[DBScanCube]] = List()
    val totallist: List[List[Int]] = List(
      List(103, 87, 41, 118, 61, 35, 142, 138, 108, 15, 133, 131, 90),
      List(139, 78, 121, 86, 18, 93, 113, 120, 110, 24, 53, 42, 79),
      List(12, 58, 143, 135, 84, 106, 123, 145, 115, 49, 137, 69),
      List(144, 81, 60, 132, 96, 129, 38, 59, 114, 72, 102, 1),
      List(119, 10, 9, 16, 105, 50, 46, 104, 74, 57, 62, 2),
      List(13, 88, 80, 21, 6, 112, 134, 3, 23, 130, 128, 97),
      List(91, 95, 32, 73, 52, 44, 8, 127, 64, 17, 29, 70),
      List(27, 99, 92, 30, 43, 67, 109, 82, 140, 7, 11, 26),
      List(36, 20, 75, 136, 77, 63, 98, 85, 89, 116, 146, 54),
      List(101, 56, 5, 51, 40, 34, 66, 22, 141, 28, 117, 14),
      List(25, 125, 47, 124, 122, 4, 126, 111, 31, 39, 68, 45),
      List(33, 71, 94, 19, 83, 48, 107, 100, 65, 55, 37, 76)
    )
    var cubelist:Set[DBScanCube]=Set()
    for(list<-totallist){
      for(v<-list){
        pointofCube.find { case (idx, cube, count) => idx == v } match {
          case Some((_, cube, count)) =>
            cubelist += cube
        }
      }
      cubepartition = cubelist :: cubepartition
//      println(cubelist)
      cubelist = Set()
    }
    cubepartition
  }

  // node bianli
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
              visited += v2
            }
            else loop.break()
          }
        }
        println("cubes",cubelist.size,"points",sum)
        cubepartition = cubelist :: cubepartition
      }
    }
    cubepartition
  }
}