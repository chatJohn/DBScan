package org.apache.spark.Scala.DBScanNaive


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
object DBScanTest{
  def main(args: Array[String]): Unit = {

    val directoryPath = "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id"
//    val fileList = Array("D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\1.txt",
//      "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id\\2.txt"
//    )
    val fileList = (100 to 110).map(i => s"$directoryPath\\$i.txt").toArray
//    val fileList = Array(args(0))
    val conf = new SparkConf()
    conf.setMaster("local[5]").setAppName("DBScan")
//    conf.setMaster("spark://startserver02:7077")
    val sparkContext: SparkContext = new SparkContext(conf)
    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 10)

    val VectorRDD: RDD[Vector] = lineRDD.map(x => {
        val strings: Array[String] = x.split(",")
        (strings(2).toDouble, strings(3).toDouble)
//        val spacestr=strings(4).replaceAll("POINT \\(([^\\s]+) ([^\\s]+)\\)", "$1,$2")
//        val spaceArray: Array[String]= spacestr.split(",")
//        (spaceArray(0).toDouble,spaceArray(1).toDouble)
    }).map((x: (Double, Double)) => {
      Vectors.dense(Array(x._1, x._2))
    })
// 400
//    val eps: Double = 0.05
//    val minPoints: Int = 80
//    val maxPointsPerPartition: Int = 400
    val eps: Double = args(2).toDouble
    val minPoints: Int = args(3).toInt
    val maxPointsPerPartition: Int = args(4).toInt
    //for get cell
    val x_bounding: Double = 0.03
    val y_bouding: Double = 0.03

    val startTime = System.currentTimeMillis()


    val DBScanRes: DBScan = DBScan.train(VectorRDD, eps, minPoints, maxPointsPerPartition,x_bounding, y_bouding, sparkContext)
    val endTime = System.currentTimeMillis()
    val total = endTime - startTime

    println(s"Total Time Cost: $total")
//    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile("D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\SparkMaven\\result")
    DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile(args(1))
    sparkContext.stop()

  }
}
