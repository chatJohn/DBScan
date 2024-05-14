package org.apache.spark.Scala.utils.sample

import org.apache.spark.Scala.DBScan3DDistributed.DBScanPoint_3D
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.SamplingUtils
import org.apache.spark.mllib.linalg.Vector
object Sample {
  def sample(rdd: RDD[Vector], sampleRate: Double): RDD[DBScanPoint_3D] = {
    val totalNum = rdd.collect().toList.size
    println("Sample Total Size: ", totalNum)
    val sampleSize = (totalNum * sampleRate).toInt
    println("Sample Size: ", sampleSize)
    //    通过计算得出的采样比
    //    val fraction = SamplingUtils.computeFractionForSampleSize(sampleSize, totalNum, false)
    //    println("Sample Fraction: ", fraction)


    val samples = rdd.sample(withReplacement = false, sampleRate, seed = 9961).map((x) => DBScanPoint_3D(x))
    samples
  }
}