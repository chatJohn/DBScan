package org.apache.spark.Scala.DBScanBloom_BitMap

import orestes.bloomfilter
import orestes.bloomfilter.FilterBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.util.control.Breaks
object CellBloomFilter{}

case class CellBloomFilter(data: RDD[Vector], allCell: Set[Rectangle]){


    def buildBloomFilter(): bloomfilter.CountingBloomFilter[String] = {
      val newCountingBloomFilter: bloomfilter.CountingBloomFilter[String] = new FilterBuilder(10000, 0.01).buildCountingBloomFilter()
      val cellIndex: Set[(Rectangle, Int)] = allCell.zipWithIndex
      var pointWithIndex: mutable.Map[Int, Int] = mutable.Map()
      val loop = new Breaks
      data.collect().foreach(x => {
        loop.breakable {
          for (elem <- cellIndex) {
            if (elem._1.contains(Point(x))) {
              if (pointWithIndex.contains(elem._2)) {
                pointWithIndex.update(elem._2, pointWithIndex.getOrElseUpdate(elem._2, 0) + 1)
              } else {
                pointWithIndex.put(elem._2, 1)
              }
              loop.break()
            }
          }
        }
      })
      val cellCountIndex: Set[((Rectangle, Int), Int)] = cellIndex.map(x => {
        ((x._1, pointWithIndex.getOrElse(x._2, 0)), x._2) // cell, the number points in cell, and the index of cell
      })

//      val sortedCellCountIndex = cellCountIndex.toSeq.sortBy(_._2)
//      sortedCellCountIndex.foreach(x=>
//          println("cellId:",x._2,"Count:",x._1._2))

      cellCountIndex.foreach(x => {
        for(i <- 0 until x._1._2){
          newCountingBloomFilter.add(x._2.toString) // put the index of the cell, which means the number of this index cell
        }
      })

      val sortedCellCountIndex = cellCountIndex.toSeq.sortBy(_._2)
      var cnt: Long=0
      sortedCellCountIndex.foreach(x=>
      if(newCountingBloomFilter.getEstimatedCount(x._2.toString)!=0){
        println("cellId:",x._2,"Point in cell:",newCountingBloomFilter.getEstimatedCount(x._2.toString))
        cnt+=newCountingBloomFilter.getEstimatedCount(x._2.toString)
      })
      println("cnt",cnt)
      newCountingBloomFilter
    }

    //都没有判断是否有核心点，只要点数达到阈值就bitmap为1了，过滤力度已经很小了，应该结果与baseline一致才对
   def getBitMap(allCell: Set[(Rectangle, Int)], countingBloomFilter: bloomfilter.CountingBloomFilter[String], eps: Double, maxPoint: Long): ArrayBuffer[Int] = {
    val bitMap: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    for (cell_i <- allCell){
      val outCell: Rectangle = cell_i._1.shrink(-eps)
      var cnt: Long = countingBloomFilter.getEstimatedCount(cell_i._2.toString)
      for (cell_j <- allCell){
        if(cell_j._2!=cell_i._2 && outCell.hasUnit(cell_j._1)){
          cnt += countingBloomFilter.getEstimatedCount(cell_j._2.toString)
        }
      }
      if(cnt >= maxPoint) bitMap += 1
      else bitMap += 0
    }
    bitMap
  }
}
