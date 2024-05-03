package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.{DBScanCube, DBScanPoint_3D}
import org.apache.spark.Scala.utils.partition.Cell_3D.getCube
import org.apache.spark.Scala.utils.partition.CellGraph_3D.getCellGraph
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
    val pointOfCube: Set[(Int, DBScanCube, Int)] = getCube(points,x_bounding,y_bounding,t_bounding)

    val cellGraph: Graph = getCellGraph(pointOfCube,x_bounding,y_bounding,t_bounding)
    println("cube graph vertices",cellGraph.vertices.size,"edges",cellGraph.edges.size)
    println("About to start partitioning...")
    val partitions = getPartition(pointOfCube,cellGraph,maxPointsPerPartition)

    println("the Partitions are below:")
    partitions.foreach(println)

    println("Partitioning Done")
    println("partitions size",partitions.size)
    println("Partitioning Done")
    partitions
  }



  // node iterable
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