package org.apache.spark.Scala.DBScan3DNaive

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object DBScan3DTest {

  def main(args: Array[String]): Unit = {
    val directoryPath = "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id"
    val fileList = Array("F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1.txt",
      "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\2.txt"
    )
    val conf = new SparkConf()
    conf.setMaster("local[5]").setAppName("DBScan")
    val sparkContext: SparkContext = new SparkContext(conf)

    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 10)

    // 以一个标准时间获取时间差
    val originDate = "2008-02-02 18:44:58"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val referDate = dateFormat.parse(originDate)
    val timestamp: Long = referDate.getTime



    val VectorRDD: RDD[Vector] = lineRDD.map(x => {
      val strings: Array[String] = x.split(",")
      val date: Date = dateFormat.parse(strings(1))
      var t: Double = date.getTime.toDouble
      t = (t - timestamp)/ 1000
      (strings(2).toDouble, strings(3).toDouble, t)
    }).map((x: (Double, Double, Double)) => {
      Vectors.dense(Array(x._1, x._2, x._3))
    })
    val distanceEps: Double = 0.05
    // new dimension: time dimension
    val timeEps: Double = 1


    val minPoints: Int = 80
    val maxPointsPerPartition: Int = 400

    val DBScanRes: DBScan3D = DBScan3D.train(VectorRDD, distanceEps, timeEps, minPoints, maxPointsPerPartition)
    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile("F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\result")
    sparkContext.stop()

  }
}
