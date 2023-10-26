package org.apache.spark.DBScanBloom_BitMap

import org.apache.hadoop.util.bloom.{CountingBloomFilter, Key}
import org.apache.spark.DBScanBloom_BitMap.CellBloomFilter.{hashType, nbHash, vectorSize}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

import scala.collection.mutable.Map
object CellBloomFilter {
  private val vectorSize: Int = Int.MaxValue
  private val nbHash: Int = 10
  private val hashType: Int = 1 // There are -1, 0, 1 three types Hash Functions Type
}


case class CellBloomFilter(data: RDD[Vector], allCell: Set[Rectangle]){
    def buildBloomFilter(): CountingBloomFilter = {
      val countingBloomFilter = new CountingBloomFilter(vectorSize, nbHash, hashType)
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
        ((x._1, pointWithIndex(x._2)), x._2)
      })
      cellCountIndex.foreach(x => {
        val key = new Key(x.toString.getBytes)
        countingBloomFilter.add(key)
      })
      countingBloomFilter
    }
}