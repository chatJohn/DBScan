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
//      .set("spark.driver.maxResultSize", "10g")
//      .set("spark.driver.memory", "6g")
      .setAppName("DBscan_3D")
      .setMaster("local[*]") // 在本地模拟运行
      //.setMaster("spark://10.242.6.19:7077") // 在分布式集群中运行
    val sparkContext: SparkContext = new SparkContext(conf)

    val fileProcess: FileProcess = FileProcess()

    // this file_list for taxi dataset
    /*
    val directoryPath = args(0)
    println(directoryPath)
    val st = args(9).toInt
    println(st)
    val en = args(10).toInt
    val fileList = (st to en).map(i => s"$directoryPath/$i.txt").toArray
     */


    // this file_list for newyork dataset
    // val fileList = fileProcess.getFileList(directoryPath)

    val fileList = Array[String](args(0))
    // specific file
    val lineRDD: RDD[String] = sparkContext.textFile(fileList.mkString(","), 5)
    val VectorRDD: RDD[Vector] = lineRDD.map((x: String) => {
      // different data format process in specific file process function
      fileProcess.NewYorkDataProcess(x)
    }).map((x: (Double, Double, Double)) => {
      Vectors.dense(Array(x._1, x._2, x._3))
    })
    println(args(2))
    val distanceEps: Double = args(2).toDouble
    // new dimension: time dimension
    val timeEps: Double = args(3).toDouble
    val minPoints: Int = args(4).toInt
    val maxPointsPerPartition: Int = args(5).toInt
    // new param: load_balance_alpha
    val load_balance_alpha = args(6).toDouble
    // new partition method params
    val x_boundind: Double = args(7).toDouble
    val y_bounding: Double = args(8).toDouble
    val t_bounding: Double = args(9).toDouble
    val startTime = System.currentTimeMillis()
    //val DBScanRes: DBScan3D = DBScan3D.train(VectorRDD, distanceEps, timeEps, minPoints, maxPointsPerPartition)
    val DBScanRes = DBScan3D_CubeSplit.train(VectorRDD, distanceEps, timeEps, minPoints, maxPointsPerPartition, load_balance_alpha, x_boundind, y_bounding, t_bounding)

    val endTime = System.currentTimeMillis()
    val total = endTime - startTime
    println(s"Total Time Cost: $total")
    //DBScanRes.labeledPoints.coalesce(1).sortBy(x => x.cluster).saveAsTextFile(args(1))
    DBScanRes.labeledPoints._1.coalesce(1).sortBy(x => x.cluster).saveAsTextFile(args(1))
    sparkContext.stop()
  }
}