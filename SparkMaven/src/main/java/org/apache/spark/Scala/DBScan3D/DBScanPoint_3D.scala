package org.apache.spark.Scala.DBScan3D

import org.apache.spark.mllib.linalg.Vector
object DBScanPoint_3D{
  val EARTH_R = 6378.137
}
case class DBScanPoint_3D(vector: Vector) {
  def distanceX: Double = vector(0)
  def distanceY: Double = vector(1)
  def timeDimension: Double = vector(2)
  def distanceSquared(other: DBScanPoint_3D): Double = {
    // 欧几里得距离
    val dx = other.distanceX - distanceX
    val dy = other.distanceY - distanceY

    dx * dx + dy * dy
  }
  def timeAbs(other: DBScanPoint_3D): Double = {
    // 时间距离
    val tx = other.timeDimension - timeDimension
    math.abs(tx)
  }
}
