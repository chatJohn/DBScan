package org.apache.spark.Scala.ScalaTest

import orestes.bloomfilter
import orestes.bloomfilter.FilterBuilder
import org.apache.hadoop.util.bloom.{CountingBloomFilter, Key}

import scala.io.Source

object DiffBloomFilterTest {
  def main(args: Array[String]): Unit = {
    val originFilter = new CountingBloomFilter(Int.MaxValue, 10, 1)
    val newFilter: bloomfilter.CountingBloomFilter[String] = new FilterBuilder(10000, 0.01).buildCountingBloomFilter()
    val filePath = "F:\\IntelliJ IDEA 2021.2.3\\JavaStudy\\SparkMaven\\src\\main\\resources\\BloomFilterTestTxt\\same_point.txt"
    val fileSource = Source.fromFile(filePath)
    for (elem <- fileSource.getLines()) {
      newFilter.add(elem)
    }
    println(newFilter.getEstimatedCount("12"))
  }
}
