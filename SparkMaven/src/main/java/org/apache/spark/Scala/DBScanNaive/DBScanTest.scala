package org.apache.spark.Scala.DBScanNaive

import org.apache.spark.Scala.DBScanNaive.DBScan
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Date

object DBScanTest {
  def main(args: Array[String]): Unit = {

    val directoryPath = "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id"
//    val fileList = (100 to 110).map(i => s"$directoryPath\\$i.txt").toArray
    //10 to 15 有孤点 100 to 110比较均匀 (100 to 106) ++ (1003 to 1009)
    val fileList = Array("D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\100.txt")
    val conf = new SparkConf()
    conf.setMaster("local[5]").setAppName("DBScan")
    val sparkContext: SparkContext = new SparkContext(conf)
    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 10)


    val dateString = "2008-02-02 18:44:58"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date0 = dateFormat.parse(dateString)
    val timestamp: Long = date0.getTime

    val VectorRDD: RDD[Vector] = lineRDD.map(x => {
      val strings: Array[String] = x.split(",")
      val date: Date = dateFormat.parse(strings(1))
      var t : Long = date.getTime
      t = (t - timestamp)/ 1000
//      println(t)
      (strings(2).toDouble, strings(3).toDouble,t.toDouble)
    }).map((x: (Double, Double,Double)) => {
      Vectors.dense(Array(x._1, x._2,x._3))
    })
    // 400
    val eps1: Double = 0.05
    val eps2: Double = 800
    val minPoints: Int = 40
    val maxPointsPerPartition: Int = 100
    //for get cell
    val x_bounding: Double = 0.03
    val y_bouding: Double = 0.03


    val startTime = System.currentTimeMillis()

    val DBScanRes: DBScan = DBScan.train(VectorRDD, eps1, eps2, minPoints, maxPointsPerPartition, x_bounding, y_bouding, sparkContext)
    val endTime = System.currentTimeMillis()
    val total = endTime - startTime
    println(s"Total Time Cost: $total")

    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile("D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\result")

    sparkContext.stop()

  }
}
