package org.apache.spark.DBScan

import org.apache.spark.mllib.linalg.Vector
object DBScanLabeledPoint {
  val Unknown = 0  //cluster

  object Flag extends Enumeration{ // 也是一个对象，但是是枚举对象
    type Flag = Value
    val Border, Core, Noise, NotFlagged = Value
  }
} // 伴生对象存放静态变量和静态方法


class DBScanLabeledPoint(vector: Vector) extends DBScanPoint(vector){
  def this(point: DBScanPoint) = this(point.vector)

  var flag = DBScanLabeledPoint.Flag.NotFlagged
  var cluster = DBScanLabeledPoint.Unknown
  var visited = false

  override def toString: String = {
    s"$vector, $cluster, $flag"
  }
}
