package org.apache.spark.DBScan

import org.apache.spark.mllib.linalg.Vector
case class DBScanPoint(vector: Vector){
  def x: Double = vector(0)
  def y: Double = vector(1)
  def distanceSquared(other: DBScanPoint): Double = {
    //欧式距离
//    val dx = other.x - x
//    val dy = other.y - y
//    dx * dx + dy * dy  //两边都不开根号，eps^2
    //经纬度距离计算 x:lon,y:lat
    var EARTH_R = 6378.137
    Math.pow(EARTH_R*Math.acos(Math.sin(other.y)*Math.sin(y)+
      Math.cos(other.y)*Math.cos(y)*Math.cos(other.x-x)),2)
  }
}
