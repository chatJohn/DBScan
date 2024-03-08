package org.apache.spark.Scala.utils.filter

import org.apache.hadoop.util.bloom.{CountingBloomFilter, Key}
import org.apache.spark.mllib.linalg.Vector

/**
 * Based the MR-DBScan algorithm, how to  use bloom filter to accelerate the efficiency of this algorithm in spark?
 *
 * Create a bloom filter for each partition of the input data.
 * Broadcast the bloom filters to all worker nodes.
 * Filter out the points that are not in the bloom filter before running MR-DBScan.
 */
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
