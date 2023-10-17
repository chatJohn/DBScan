package org.apache.spark.DBScan

import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
object DBScanTest{
  def main(args: Array[String]): Unit = {
    val fileList = Array("C:\\Users\\Administrator\\Desktop\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1.txt",
      "C:\\Users\\Administrator\\Desktop\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\2.txt",
      "C:\\Users\\Administrator\\Desktop\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\3.txt")
//    val fileList = Array("C:\\Users\\Administrator\\Desktop\\SparkMaven\\src\\main\\resources\\small.txt")
    val conf = new SparkConf()
    conf.setMaster("local[5]").setAppName("DBScan")
    val sparkContext = new SparkContext(conf)
    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 10)
    val VectorRDD: RDD[Vector] = lineRDD.map(x => {
      val strings: Array[String] = x.split(",")
      (strings(2).toDouble, strings(3).toDouble)  //要根据txt文件修改
    }).map((x: (Double, Double)) => {
      Vectors.dense(Array(x._1, x._2))
    })

    val eps: Double = 0.00006 //2
    val minPoints: Int = 4
    val maxPointsPerPartition: Int = 600//20

    val DBScanRes: DBScan = DBScan.train(VectorRDD, eps, minPoints, maxPointsPerPartition)
    println(DBScanRes)

    sparkContext.stop()
  }
}
