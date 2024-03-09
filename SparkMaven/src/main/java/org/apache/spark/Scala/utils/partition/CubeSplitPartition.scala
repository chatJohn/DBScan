package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.DBScanCube
import org.apache.spark.Scala.utils.partition.Cell_3D.getCube
import org.apache.spark.Scala.utils.partition.CellGraph_3D.getcellGraph

object CubeSplitPartition{
  def getPartition(pointCube:Set[(DBScanCube, Int)], bounding: Double): List[(DBScanCube, Int)] = {
    new CubeSplitPartition(pointCube, bounding).getSplits()
  }
}


case class CubeSplitPartition(pointCube:Set[(DBScanCube, Int)], bounding: Double) {
  def getSplits(): List[(DBScanCube, Int)]={
    val pointofCube:Set[(Int, DBScanCube, Int)] = getCube(pointCube, bounding)
    val cellgraph:Graph = getcellGraph(pointofCube,bounding)
    
  }

}