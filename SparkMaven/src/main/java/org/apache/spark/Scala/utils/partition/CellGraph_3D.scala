package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.DBScanCube
import scala.collection.mutable
import scala.math.sqrt

case class Graph(vertices: Set[Int], edges: Map[(Int, Int), Double])

object CellGraph_3D{

  def getcellGraph(pointofCube:Set[(Int, DBScanCube, Int)],bounding:Double): Graph = {
    new CellGraph_3D(pointofCube,bounding).getGraph()
  }
  println("sssss")

}


case class CellGraph_3D(pointofCube:Set[(Int, DBScanCube, Int)],bounding:Double) {
  def neighbor(cube1: DBScanCube, cube2: DBScanCube): Boolean ={
    val dx = Math.abs(cube1.x - cube2.x)
    val dy = Math.abs(cube1.y - cube2.y)
    val dt = Math.abs(cube1.t - cube2.t)
    dx <= bounding && dy <= bounding && dt <= bounding
  }

  def getGraph(): Graph={
    var vertices: Set[Int] = Set()    //
    var edges: mutable.Map[(Int, Int), Double] = mutable.Map()
    for {
      (id1, cube1, count1) <- pointofCube
      (id2, cube2, count2) <- pointofCube if id1 < id2 // 避免重复边和自环
    } {
      if(neighbor(cube1,cube2)){
        // 计算权重
        val weight = sqrt(count1 * count2)

        vertices += id1
        vertices += id2

        edges += (id1, id2) -> weight
      }
    }
    //不可变映射
    val immutableEdges = edges.toMap

    Graph(vertices, immutableEdges)
  }
}