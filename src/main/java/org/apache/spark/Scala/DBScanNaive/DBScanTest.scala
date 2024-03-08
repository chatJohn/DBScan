package org.apache.spark.Scala.DBScanNaive


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import java.text.SimpleDateFormat
import java.util.Date
object DBScanTest{
  def main(args: Array[String]): Unit = {
    val directoryPath = "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id"
    val fileList = Array("F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1.txt",
                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\2.txt"
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\3.txt",
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\4.txt",
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\5.txt",
//
//
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1026.txt",
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1027.txt",
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1028.txt",
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1029.txt",
//
//
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\6732.txt",
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\6733.txt",
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\6738.txt",
//                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\6744.txt"

    )


    val conf = new SparkConf()
    conf.setMaster("local[5]").setAppName("DBScan")
    val sparkContext: SparkContext = new SparkContext(conf)
    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 10)

    val VectorRDD: RDD[Vector] = lineRDD.map(x => {
      val strings: Array[String] = x.split(",")

      (strings(2).toDouble, strings(3).toDouble)
    }).map((x: (Double, Double)) => {
      Vectors.dense(Array(x._1, x._2))
    })
    // 400
    val eps: Double = 0.05



    val minPoints: Int = 80
    val maxPointsPerPartition: Int = 400
    // for get cell, this parameter is important for the efficiency of this spark app, and this should be tested!!!
    val x_bounding: Double = 0.01
    val y_bounding: Double = 0.01

    val startTime = System.currentTimeMillis()

    val DBScanRes: DBScan = DBScan.train(VectorRDD, eps, minPoints, maxPointsPerPartition,x_bounding, y_bounding, sparkContext)
    val endTime = System.currentTimeMillis()
    val total = endTime - startTime

    println(s"Total Time Cost: $total")
    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile("F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\result")

    sparkContext.stop()

  }
}
