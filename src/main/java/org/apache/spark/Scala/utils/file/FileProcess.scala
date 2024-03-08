package org.apache.spark.Scala.utils.file

import org.apache.spark.Scala.utils.file.FileProcess.{Date_Format, Date_Regex, Original_Date, Point_Regex, Position_Regex}

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

object FileProcess{
  val Original_Date = "2008-02-02 18:44:58"
  val Date_Format = "yyyy-MM-dd HH:mm:ss"
  val Date_Regex = """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}""".r
  val Point_Regex = """POINT \(([^\s]+) ([^\s]+)\)""".r
  val Position_Regex = """[(][^\s]+ [^\s]+[)]""".r
}

case class FileProcess() {
  def getFileList(directoryPath: String): Array[String] = {
    val filePath = new File(directoryPath)
    val files: Array[File] = filePath.listFiles()
    var fileList = Array[String]()
    if(files != null){
      for(file <- files){
        fileList = fileList :+ file.getPath
      }
    }
    fileList
  }
  def DataProcess(line: String):  (Double, Double, Double) = {
    val dateFormat = new SimpleDateFormat(Date_Format)
    val referDate: Date = dateFormat.parse(Original_Date)
    val timestamp: Long = referDate.getTime // 毫秒
   // println("parse string:", (Date_Regex findFirstIn (line) toString).replace("Some(", "").replace(")", ""))
    val date: Date = dateFormat.parse((Date_Regex findFirstIn (line) toString).
                      replace("Some(", "").replace(")", ""))
    var t: Double = date.getTime.toDouble
    t = (t - timestamp) / 100000
    val spaceStr: String = (Position_Regex findFirstIn(Point_Regex findFirstIn(line) toString) toString)
                          .replace("Some((", "")
                          .replace(")))", "")

//    println("space str: ", spaceStr)
    val spaceArr = spaceStr.split(" ")
//    println("space Arr: ", spaceArr(0))
    (spaceArr(0).toDouble, spaceArr(1).toDouble, t)
  }
}
