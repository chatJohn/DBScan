package com.chatting.spark.BF

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.sketch.BloomFilter

/**
 * BloomFilter的基础参数
 * m - the length of bitmap
 * n - the elements counts which should storage
 * f - false positive, 损失精度
 */
object test1 {
  def main(args: Array[String]): Unit = {
    val bloomFilter = BloomFilter.create(100, 0.2)
    bloomFilter.put("chatting")
    bloomFilter.put("john")
    val bool = bloomFilter.mightContain("chatting")
    println(bool)
  }
}
