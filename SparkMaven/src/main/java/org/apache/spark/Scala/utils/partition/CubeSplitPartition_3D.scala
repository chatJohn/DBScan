package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DDistributed.{DBScanCube, DBScanPoint_3D}
import org.apache.spark.Scala.utils.partition.Cell_3D.getCube
import org.apache.spark.Scala.utils.partition.CellGraph_3D.getcellGraph
import org.apache.spark.Scala.utils.partition.Kernighan_Lin.getPartition
import org.apache.spark.Scala.utils.partition.Greedy.getGreedyPartition
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

    val cellgraph: Graph = getcellGraph(pointofCube,x_bounding,y_bounding,t_bounding)
    println("cube graph vertices",cellgraph.vertices.size,"edges",cellgraph.edges.size)

    println("About to start partitioning...")
//    val partitions = edgepartition(cellgraph, pointofCube)
//    val partitions = partition1(pointofCube)
    val partitions = getPartition(pointofCube,cellgraph,maxPointsPerPartition)
//    val temp = points.size/maxPointsPerPartition
//    println(temp)
//    val partitions = getGreedyPartition(pointofCube,cellgraph,temp,maxPointsPerPartition)
    println("the Partitions are below:")
    partitions.foreach(println)

    println("Partitioning Done")
    println("partitions size",partitions.size)
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
  def nodepartition(cellgraph: Graph, pointofCube: Set[(Int, DBScanCube, Int)]):List[Set[DBScanCube]] = {
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


  def idcountsum(pointofCube: Set[(Int, DBScanCube, Int)],cubeset:Set[Int]): Int ={
    var sum = 0
    for(cubeid<-cubeset){
      pointofCube.find { case (idx, cube, count) => idx == cubeid } match {
        case Some((_, cube, count)) =>
          sum += count
      }
    }
    sum
  }
  // edge bianli
  def edgepartition(cellgraph: Graph, pointofCube: Set[(Int, DBScanCube, Int)]): List[Set[DBScanCube]] = {
    // 创建一个映射，将点映射到它所在的集合
    val pointToSet = mutable.Map[Int, Set[Int]]()
    pointofCube.foreach { case (id, _, _) =>
      pointToSet(id) = Set(id)
    }

    // 遍历图中的每条边
    for (((id1, id2),_) <- cellgraph.edges) {
      if(id1 >= 0 && id2 >= 0){
        val set1 = pointToSet(id1)
        val set2 = pointToSet(id2)
        // 如果边相邻的两个集合不相等，且它们合并后的大小不超过阈值，则合并
        if (set1 != set2 && (idcountsum(pointofCube,set1) + idcountsum(pointofCube,set2)) <= maxPointsPerPartition) {
          val mergedSet = set1 ++ set2
          mergedSet.foreach { cube =>
            pointToSet(cube) = mergedSet
          }
        }
      }
    }
    val cubeid = pointToSet.values.toList.distinct
    var cubepartition: List[Set[DBScanCube]] = List() //每个分区由若干个Cube组成
    var summax:Int = 0
    var summin:Int = Int.MaxValue
    var sum = 0
    for(cubeidset <- cubeid){
      var cubelist:Set[DBScanCube]=Set()
      for(cubeid <- cubeidset){
        pointofCube.find { case (idx, _, _) => idx == cubeid } match {
          case Some((_, cube, count)) =>
            cubelist += cube
            sum += count
        }
      }
      print(sum,"")
      if(sum>summax) summax = sum
      if(sum<summin) summin = sum
      sum = 0
      cubepartition = cubelist :: cubepartition
    }
    println("points in partion max-min: ",summax-summin)
    cubepartition
  }
}