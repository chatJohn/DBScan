package org.apache.spark.Scala.DBScanNaive

import org.apache.spark.Scala.DBScanNaive.DBScan
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DBScanTest {
  def main(args: Array[String]): Unit = {

//    val fileList = Array("D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1.txt",
//      "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\2.txt",
//      "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\3.txt",
//      "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\4.txt",
//      "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\5.txt",
//      "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\6.txt"
//    )
    val directoryPath = "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id"
    val fileList = (100 to 110).map(i => s"$directoryPath\\$i.txt").toArray
    //10 to 15 有孤点

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
    val minPoints: Int = 60
    val maxPointsPerPartition: Int = 500
    //for get cell
    val x_bounding: Double = 0.02
    val y_bouding: Double = 0.02

    val startTime = System.currentTimeMillis()


    val DBScanRes: DBScan = DBScan.train(VectorRDD, eps, minPoints, maxPointsPerPartition, x_bounding, y_bouding, sparkContext)
    val endTime = System.currentTimeMillis()
    val total = endTime - startTime

    println(s"Total Time Cost: $total")
    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile("D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\result")

    sparkContext.stop()

  }
}
