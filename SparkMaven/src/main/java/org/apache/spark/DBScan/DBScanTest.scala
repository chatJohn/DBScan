package org.apache.spark.DBScan

import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
object DBScanTest{
  def main(args: Array[String]): Unit = {

    val fileList = Array("F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1.txt",
                          "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\2.txt",
                        "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\3.txt")


    val conf = new SparkConf()
    conf.setMaster("local[5]").setAppName("DBScan")
    val sparkContext = new SparkContext(conf)
    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 10)

    val VectorRDD: RDD[Vector] = lineRDD.map(x => {
      val strings: Array[String] = x.split(",")
      (strings(2).toDouble, strings(3).toDouble)
    }).map((x: (Double, Double)) => {
      Vectors.dense(Array(x._1, x._2))
    })

    val eps: Double = 0.0005
    val minPoints: Int = 60
    val maxPointsPerPartition: Int = 100

    val DBScanRes: DBScan = DBScan.train(VectorRDD, eps, minPoints, maxPointsPerPartition)
    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile("F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\result")
 

    sparkContext.stop()
  }
}
