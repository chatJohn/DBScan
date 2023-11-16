package org.apache.spark.Scala.DBScanBloom_BitMap

import orestes.bloomfilter
import orestes.bloomfilter.FilterBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

object CellBloomFilter{}

case class CellBloomFilter(data: RDD[Vector], allCell: Set[Rectangle]){
    def buildBloomFilter(): bloomfilter.CountingBloomFilter[String] = {

      val newCountingBloomFilter: bloomfilter.CountingBloomFilter[String] = new FilterBuilder(10000, 0.01).buildCountingBloomFilter()
      val cellIndex: Set[(Rectangle, Int)] = allCell.zipWithIndex
      val pointWithIndex = Map[Int, Int]()

      data.collect().foreach(x => {
        for (elem <- cellIndex) {
          if(elem._1.contains(Point(x))){
            if(pointWithIndex.contains(elem._2)){
              pointWithIndex(elem._2) += 1
            }else{
              pointWithIndex += (elem._2 -> 1)
            }
          }
        }
      })
      val cellCountIndex: Set[((Rectangle, Int), Int)] = cellIndex.map(x => {
        ((x._1, pointWithIndex(x._2)), x._2) // cell, the number points in cell, and the index of cell
      })
      cellCountIndex.foreach(x => {
        for(i <- 0 until x._1._2){
          newCountingBloomFilter.add(x._2.toString()) // put the index of the cell, which means the number of this index cell
        }
      })
      newCountingBloomFilter
    }

  private def getBitMap(allCell: Set[(Rectangle, Int)], countingBloomFilter: bloomfilter.CountingBloomFilter[String], eps: Double, maxPoint: Long): ArrayBuffer[Int] = {
    val bitMap: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    for (cell_i <- allCell){
      val outCell: Rectangle = cell_i._1.shrink(eps)
      var cnt: Long = countingBloomFilter.getEstimatedCount(cell_i._2.toString)
      for (cell_j <- allCell){
        if(outCell.hasUnit(cell_j._1)){
          cnt += countingBloomFilter.getEstimatedCount(cell_j._2.toString)
        }
      }
      if(cnt >= maxPoint) bitMap += 1
      else bitMap += 0
    }
    bitMap
  }
}