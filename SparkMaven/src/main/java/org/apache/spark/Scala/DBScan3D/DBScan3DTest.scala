package org.apache.spark.Scala.DBScan3D

import scala.io.Source
import java.text.SimpleDateFormat
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.io.PrintWriter
import java.io.File

object DBScan3DTest {

  def main(args: Array[String]): Unit = {
    val directoryPath = "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id"
    val fileList = (100 to 110).map(i => s"$directoryPath\\$i.txt").toArray

    val originDate = "2018-10-01 00:30:00"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val referDate = dateFormat.parse(originDate)
    val timestamp: Long = referDate.getTime

    val VectorRDD: Array[Vector] = fileList.flatMap { file =>
      val lines = Source.fromFile(file).getLines()
      lines.map { line =>
        val strings = line.split(",")
        val date = dateFormat.parse(strings(1))
        var t: Double = date.getTime.toDouble
        t = (t - timestamp) / 100000
        Vectors.dense(Array(strings(2).toDouble, strings(3).toDouble, t))
      }
    }.toArray

    val distanceEps: Double = args(2).toDouble
    val timeEps: Double = args(3).toDouble
    val minPoints: Int = args(4).toInt

    val startTime = System.currentTimeMillis()
    val DBScanRes: DBScan3D_cubesplit = DBScan3D_cubesplit.train(VectorRDD, distanceEps, timeEps, minPoints)
    val endTime = System.currentTimeMillis()
    val total = endTime - startTime
    println(s"Total Time Cost: $total")

    val resultPath = "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\result\\result.txt"
//    val resultPath = args(1)
    val resultFile = new File(resultPath)
    val writer = new PrintWriter(resultFile)
    try {
      DBScanRes.labeledPoints.foreach { point =>
        writer.println(point.toString) // 将对象转换为字符串并写入文件
      }
    } finally {
      writer.close()
    }
  }
}
