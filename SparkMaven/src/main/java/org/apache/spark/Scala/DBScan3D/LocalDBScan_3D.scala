package org.apache.spark.Scala.DBScan3D

import org.apache.spark.Scala.DBScan3D.DBScanLabeledPoint_3D.Flag
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

}
