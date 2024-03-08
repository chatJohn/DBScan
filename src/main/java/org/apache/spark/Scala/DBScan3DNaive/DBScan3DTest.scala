package org.apache.spark.Scala.DBScan3DNaive


import org.apache.spark.Scala.utils.file.{FileProcess}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object DBScan3DTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

      .set("spark.driver.maxResultSize", "10g")
      .set("spark.driver.memory", "6g")
      .setAppName("DBscan_3D")
      .setMaster("local[*]") // 在本地模拟运行
//      .setMaster("spark://startserver02:7077") // 在分布式集群中运行
    val sparkContext: SparkContext = new SparkContext(conf)
    val fileList: Array[String] = Array[String](args(0)) // Spark中单纯读取一个文件
    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 10)

    val VectorRDD: RDD[Vector] = lineRDD.map((x: String) => {
      new FileProcess().DataProcess(x)
    }).map((x: (Double, Double, Double)) => {
      Vectors.dense(Array(x._1, x._2, x._3))
    })
    val distanceEps: Double = args(2).toDouble
    // new dimension: time dimension
    val timeEps: Double = args(3).toDouble
    val minPoints: Int = args(4).toInt
    val maxPointsPerPartition: Int = args(5).toInt
    val DBScanRes: DBScan3D = DBScan3D.train(VectorRDD, distanceEps, timeEps, minPoints, maxPointsPerPartition)
    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile(args(1))
    sparkContext.stop()
  }
}
