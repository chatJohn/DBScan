package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.DBScanCube
import scala.collection.mutable
import scala.math.sqrt

case class Graph(vertices: mutable.SortedSet[Int], edges: Map[(Int, Int), Double])

object CellGraph_3D{

  def getCellGraph(pointofCube:Set[(Int, DBScanCube, Int)], x_bounding:Double, y_bounding: Double, t_bounding: Double): Graph = {
    new CellGraph_3D(pointofCube, x_bounding, y_bounding, t_bounding).getGraph()
  }

}


case class CellGraph_3D(pointofCube:Set[(Int, DBScanCube, Int)], x_bounding:Double, y_bounding: Double, t_bounding: Double) {
  def neighbor(cube1: DBScanCube, cube2: DBScanCube): Boolean ={
    val dx = Math.abs(cube1.x - cube2.x)
    val dy = Math.abs(cube1.y - cube2.y)
    val dt = Math.abs(cube1.t - cube2.t)
    dx <= x_bounding && dy <= y_bounding && dt <= t_bounding
  }

  def getGraph(): Graph={
    var vertices: mutable.SortedSet[Int] = mutable.SortedSet()
    var edges: mutable.Map[(Int, Int), Double] = mutable.Map()
    // create temp structure which contains some isolated cubes
    var IsolatedCubeSet: mutable.Set[(Int, DBScanCube, Int)] = mutable.Set[(Int, DBScanCube, Int)]()
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
        if(IsolatedCubeSet.contains((id2, cube2, count2))){
          IsolatedCubeSet.remove((id2, cube2, count2))
        }
      }else{ // there are not cubes around someone cube, so this cube is isolated cube, eg. id1 and id2 is not neighbor
        IsolatedCubeSet.add((id2, cube2, count2))
      }
    }
    if(IsolatedCubeSet.size != 0){
      IsolatedCubeSet.foreach(x => {
        vertices += x._1
        edges += (-1, x._1) -> 0 // -1 means not neighbors, and it's weight is 0
      })
    }
    //不可变映射
    val immutableEdges = edges.toMap

    Graph(vertices, immutableEdges)
  }
}