package org.apache.spark.Scala.ScalaTest

import java.io.File

object FIleReadTest {
  def main(args: Array[String]): Unit = {
    val directoryPath = "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\taxi_log_2008_by_id"
    val filePath = new File(directoryPath)
    val files: Array[File] = filePath.listFiles()
    var fileList = Array[String]()
    if(files != null){
      for(file <- files){
        fileList = fileList :+ file.getPath()
      }
    }
    for(i <- fileList){
      println(i)
    }
  }
}
