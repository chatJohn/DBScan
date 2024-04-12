package org.apache.spark.Scala.DBScan3D

import org.apache.spark.Scala.DBScan3D.DBScanLabeledPoint_3D.Flag
import org.apache.spark.Scala.DBScan3D.LocalDBScan_3D
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.internal.Logging

object DBScan3D_cubesplit{
  def train(data: Array[Vector],
            distanceEps: Double,
            timeEps: Double,
            minPoints: Int
           ): DBScan3D_cubesplit = {
    new DBScan3D_cubesplit(distanceEps, timeEps, minPoints,null).train(data)
  }
}

class DBScan3D_cubesplit private(val distanceEps: Double,
                                 val timeEps: Double,
                                 val minPoints: Int,
                                 @transient private val labeledPointsall: Array[DBScanLabeledPoint_3D])
  extends Serializable with  Logging{

  def labeledPoints: Array[DBScanLabeledPoint_3D] = labeledPointsall

  private def train(data: Array[Vector]): DBScan3D_cubesplit = {

    val points: Iterable[DBScanPoint_3D] = data.map(DBScanPoint_3D(_))
    println("points.size",points.size)

    val dbscan = new LocalDBScan_3D(distanceEps, timeEps, minPoints)
    val clustered: Iterable[DBScanLabeledPoint_3D] = dbscan.fit(points)

//    val clusterSizes = clustered.groupBy(_.cluster).mapValues(_.size)
//    clusterSizes.foreach { case (cluster, size) =>
//      println(s"Cluster $cluster has $size points.")
//    }

    println("Done")
    new DBScan3D_cubesplit(
      distanceEps,
      timeEps,
      minPoints,
      clustered.toArray)
  }
}
