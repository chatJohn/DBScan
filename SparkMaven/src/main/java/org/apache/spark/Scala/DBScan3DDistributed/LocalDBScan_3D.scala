package org.apache.spark.Scala.DBScan3DDistributed

import org.apache.spark.Scala.DBScan3DDistributed.DBScanLabeledPoint_3D.Flag
import org.apache.spark.internal.Logging

import scala.collection.mutable


class LocalDBScan_3D(distanceEps: Double, timeEps: Double, minPoints: Int) extends Logging{
  val minDistanceSquared = distanceEps * distanceEps
  val minTimeAbs = timeEps

  def fit(points: Iterable[DBScanPoint_3D]): Iterable[DBScanLabeledPoint_3D] = {
    println("About to start fitting")
    val labeledPoints = points.map(new DBScanLabeledPoint_3D(_)).toArray

    val totalClusters = labeledPoints.foldLeft(DBScanLabeledPoint_3D.Unknown)(
      (cluster, point) => {
        if (!point.visited) {
          point.visited = true
          val neighbors = findNeighbors(point, labeledPoints)
          if (neighbors.size < minPoints) {
            point.flag = Flag.Noise
            cluster
          } else {
            println("About to expand the cluster")
            expandCluster(point, neighbors, labeledPoints, cluster + 1)
            cluster + 1
          }
        } else {
          cluster
        }
      }
    )
    println(s"found: $totalClusters clusters")
    labeledPoints
  }

  private def findNeighbors(point: DBScanPoint_3D, all: Array[DBScanLabeledPoint_3D]): Iterable[DBScanLabeledPoint_3D] = {
    all.view.filter(other => {
      point.distanceSquared(other) <= minDistanceSquared && point.timeAbs(other) <= minTimeAbs
    })
  }

  def expandCluster(point: DBScanLabeledPoint_3D,
                    neighbors: Iterable[DBScanLabeledPoint_3D],
                    all: Array[DBScanLabeledPoint_3D],
                    cluster: Int): Unit = {
    point.flag = Flag.Core
    point.cluster = cluster

    var allNeighbors = mutable.Queue(neighbors)
    while(allNeighbors.nonEmpty){
      allNeighbors.dequeue().foreach(neighobr => {
        if(!neighobr.visited){
          neighobr.visited = true
          neighobr.cluster = cluster
          val neighborNeighbors = findNeighbors(neighobr, all)
          if(neighborNeighbors.size >= minPoints){
            neighobr.flag = Flag.Core
            allNeighbors.enqueue(neighborNeighbors)
          }else{
            neighobr.flag = Flag.Border
          }
        }
        else if(neighobr.cluster == DBScanLabeledPoint_3D.Unknown){
          neighobr.cluster = cluster
          neighobr.flag = Flag.Border
        } // point was labeled is Noise before, so the cluster is Unknown, but it can be the Border which matches the other core point
      })
    }
  }
//  def expandCluster(point: DBScanLabeledPoint_3D,
//                    neighbors: Iterable[DBScanLabeledPoint_3D],
//                    all: Array[DBScanLabeledPoint_3D],
//                    cluster: Int): Unit = {
//    point.flag = Flag.Core
//    point.cluster = cluster
//
//    // 创建一个队列，用于存储待处理的邻居
//    val neighborQueue = mutable.Queue(neighbors.toSeq: _*)
//
//    while (neighborQueue.nonEmpty) {
//      // 从队列中获取下一个邻居
//      val neighbor = neighborQueue.dequeue()
//
//      if (!neighbor.visited) {
//        neighbor.visited = true
//        neighbor.cluster = cluster
//
//        // 查找当前邻居的邻居
//        val neighborNeighbors = findNeighbors(neighbor, all)
//
//        if (neighborNeighbors.size >= minPoints) {
//          neighbor.flag = Flag.Core
//          // 将当前邻居的邻居加入队列
//          neighborQueue.enqueue(neighborNeighbors.toSeq: _*)
//        } else {
//          neighbor.flag = Flag.Border
//        }
//      } else if (neighbor.cluster == DBScanLabeledPoint_3D.Unknown) {
//        // 如果邻居之前被标记为 Noise，则更新其簇和标志
//        neighbor.cluster = cluster
//        neighbor.flag = Flag.Border
//      }
//    }
//  }

}
