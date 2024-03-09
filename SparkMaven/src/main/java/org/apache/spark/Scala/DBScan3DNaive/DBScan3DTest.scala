package org.apache.spark.Scala.DBScan3DNaive

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object DBScan3DTest {
//spark submit --数据集路径 --result路径 --distanceEps --timeEps --minPoints --maxPointsPerPartition
  def main(args: Array[String]): Unit = {
//    val directoryPath = "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id"
//    val fileList = Array("D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1.txt",
//      "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\2.txt"
//    )
    val fileList = Array(args(0))
//    val fileList = ("D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\point_r_10w")
    val conf = new SparkConf()
    conf.setMaster("local[5]").setAppName("DBScan")
//    conf.setMaster("spark://startserver02:7077")
    val sparkContext: SparkContext = new SparkContext(conf)

    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 10)

    // 以一个标准时间获取时间差2008-02-02 18:44:58
    val originDate = "2018-10-01 00:30:00"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val referDate = dateFormat.parse(originDate)
    val timestamp: Long = referDate.getTime

    val VectorRDD: RDD[Vector] = lineRDD.map(x => {
      val strings: Array[String] = x.split(",")
//      val date: Date = dateFormat.parse(strings(1))
      // New York
//      val date: Date = dateFormat.parse(strings(3))
      //chengdu
      val date: Date = dateFormat.parse(strings(3))
      var t: Double = date.getTime.toDouble
      t = (t - timestamp)/ 100000

//      (strings(2).toDouble, strings(3).toDouble, t)
      // New York
//      (strings(2).toDouble, strings(3).toDouble, t)
      //chengdu
      val spacestr=strings(4).replaceAll("POINT \\(([^\\s]+) ([^\\s]+)\\)", "$1,$2")
      val spaceArray: Array[String]= spacestr.split(",")
      (spaceArray(0).toDouble,spaceArray(1).toDouble, t)

    }).map((x: (Double, Double, Double)) => {
      Vectors.dense(Array(x._1, x._2, x._3))
    })
//    val distanceEps: Double = 0.08
//    // new dimension: time dimension
//    val timeEps: Double = 200
//    val minPoints: Int = 40
//    val maxPointsPerPartition: Int = 400
    val distanceEps: Double = args(2).toDouble
    val timeEps: Double = args(3).toDouble
    val minPoints: Int = args(4).toInt
    val maxPointsPerPartition: Int = args(5).toInt

    val startTime = System.currentTimeMillis()
    val DBScanRes: DBScan3D = DBScan3D.train(VectorRDD, distanceEps, timeEps, minPoints, maxPointsPerPartition)
    val endTime = System.currentTimeMillis()
    val total = endTime - startTime
    println(s"Total Time Cost: $total")

//    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile("D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\result")
    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile(args(1))
    sparkContext.stop()
  }
}