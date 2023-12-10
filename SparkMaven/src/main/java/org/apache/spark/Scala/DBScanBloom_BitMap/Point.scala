package org.apache.spark.Scala.DBScanBloom_BitMap

import org.apache.spark.Scala.DBScanBloom_BitMap.Point.EARTH_R
import org.apache.spark.mllib.linalg.Vector
object Point{
  val EARTH_R = 6378.137
}
case class Point(vector: Vector){
  def x: Double = vector(0)
  def y: Double = vector(1)
  def distanceSquared(other: Point): Double = {
  // 欧几里得距离
    val dx = other.x - x
    val dy = other.y - y
    dx * dx + dy * dy


    // 经纬度计算距离
    // x: lon, y: lan

//    Math.pow(EARTH_R * Math.acos(Math.sin(other.y) * Math.sin(y) +
//              Math.cos(other.y) * Math.cos(y) * Math.cos(other.x - x)), 2)
  }
}
