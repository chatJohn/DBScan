package org.apache.spark.Scala.DBScan3D

import org.apache.spark.mllib.linalg.Vector
object DBScanLabeledPoint_3D {
  val Unknown = 0

  object Flag extends Enumeration{ // 也是一个对象，但是是枚举对象
    type Flag = Value
    val Border, Core, Noise, NotFlagged = Value // id  = 0, 1, 2, 3
  }
} // 伴生对象存放静态变量和静态方法


class DBScanLabeledPoint_3D(vector: Vector) extends DBScanPoint_3D(vector){
  def this(point: DBScanPoint_3D) = this(point.vector)

  var flag = DBScanLabeledPoint_3D.Flag.NotFlagged
  var cluster = DBScanLabeledPoint_3D.Unknown
  var visited = false

  override def toString: String = {
    s"$vector, $cluster, $flag"
  }
}
