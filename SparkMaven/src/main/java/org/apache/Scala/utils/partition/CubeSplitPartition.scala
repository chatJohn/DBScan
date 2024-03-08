package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.DBScanCube


object CubeSplitPartition{
  /**
   * Split the giving Space into some Cubes
   * @param bounding
   * @return the Set of Cubes
   */
  def getCube(pointCube:Set[DBScanCube], bounding: Double): Set[DBScanCube] = {
    new CubeSplitPartition(pointCube, bounding).getSplits() // doing...
  }
}


case class CubeSplitPartition(pointCube: Set[DBScanCube], bounding: Double) {

  def getMinimalBounding(pointCube: Set[DBScanCube]): DBScanCube = {
    val x_min: Double = pointCube.map(x => x.x).toList.min
    val x_max: Double = pointCube.map(x => x.x2).toList.max
    val y_min: Double = pointCube.map(x => x.y).toList.min
    val y_max: Double = pointCube.map(x => x.y2).toList.max
    val t_min: Double = pointCube.map(x => x.t).toList.min
    val t_max: Double = pointCube.map(x => x.t2).toList.max
    DBScanCube(x_min, y_min, t_min, x_max, y_max, t_max)
  }
  def getSplits(): Set[DBScanCube] ={
    val boundingCube: DBScanCube = getMinimalBounding(pointCube)
    doSplitWithBounding(boundingCube, bounding)
  }
  def doSplitWithBounding(originCube: DBScanCube, bounding: Double): Set[DBScanCube] = {
    val x_split = (originCube.x until originCube.x2 by bounding).toList :+ originCube.x2
    val y_split = (originCube.y until originCube.y2 by bounding).toList :+ originCube.y2
    val t_split = (originCube.t until originCube.t2 by bounding).toList :+ originCube.t2
    // n^3 ? high cost of complexity
    val cubes = for {
      x <- x_split.init
      y <- y_split.init
      t <- t_split.init
    } yield DBScanCube(x, y, t, x + bounding, y + bounding, t + bounding)
    cubes.toSet
  }
}
