package org.apache.Scala.utils.filter

import org.apache.hadoop.util.bloom.{CountingBloomFilter, Key}
import org.apache.spark.mllib.linalg.Vector

object BloomFilter{
  private val vectorSize: Int = Int.MaxValue
  private val nbHash: Int = 10
  private val hashType: Int = 1 // There are -1, 0, 1 three types Hash Functions Type
  val filter = new CountingBloomFilter(vectorSize, nbHash, hashType)
}

case class BloomFilter(vector: Vector) {
  def x = vector(0)

  def y = vector(1)

  def key: Key = new Key(Tuple2(x, y).toString().getBytes())
}
