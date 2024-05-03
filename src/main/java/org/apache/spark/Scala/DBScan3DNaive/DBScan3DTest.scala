package org.apache.spark.Scala.DBScan3DNaive


import org.apache.spark.Scala.utils.file.FileProcess
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
    // val fileList: Array[String] = Array[String](args(0)) // Spark中单纯读取一个文件

    // specific file
    val directoryPath = "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id"
    val fileList = (100 to 110).map(i => s"$directoryPath\\$i.txt").toArray
    // 以一个标准时间获取时间差2008-02-02 18:44:58
    val originDate = "2018-10-01 00:30:00"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val referDate = dateFormat.parse(originDate)
    val timestamp: Long = referDate.getTime
    // specific file

    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 10)
    val VectorRDD: RDD[Vector] = lineRDD.map((x: String) => {
//      new FileProcess().DataProcess(x)
    val strings: Array[String] = x.split(",")
      val date: Date = dateFormat.parse(strings(1))
      var t: Double = date.getTime.toDouble
      t = (t - timestamp)/ 100000
      (strings(2).toDouble, strings(3).toDouble, t)
    }).map((x: (Double, Double, Double)) => {
      Vectors.dense(Array(x._1, x._2, x._3))
    })
    val distanceEps: Double = args(2).toDouble
    // new dimension: time dimension
    val timeEps: Double = args(3).toDouble
    val minPoints: Int = args(4).toInt
    val maxPointsPerPartition: Int = args(5).toInt
    // new partition method params
    val x_boundind: Double = args(6).toDouble
    val y_bounding: Double = args(7).toDouble
    val t_bounding: Double = args(8).toDouble

//    val DBScanRes: DBScan3D = DBScan3D.train(VectorRDD, distanceEps, timeEps, minPoints, maxPointsPerPartition)
    val DBScanRes: DBScan3D_CubeSplit = DBScan3D_CubeSplit.train(VectorRDD, distanceEps, timeEps, minPoints, maxPointsPerPartition, x_boundind, y_bounding, t_bounding)
    // DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile(args(1))
    sparkContext.stop()
  }
}
