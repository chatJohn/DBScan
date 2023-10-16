package org.apache.spark.DBScan

import org.apache.spark.mllib.linalg.Vector
case class DBScanPoint(vector: Vector){
  def x: Double = vector(0)
  def y: Double = vector(1)
  def distanceSquared(other: DBScanPoint): Double = {
    val dx = other.x - x
    val dy = other.y - y
    dx * dx + dy * dy
  }
}
