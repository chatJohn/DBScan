package org.apache.spark.Scala.utils.partition

import org.apache.spark.Scala.DBScan3DNaive.DBScanCube


object Cell_3D{
  /**
   * Split the giving Space into some Cubes
   * @param bounding
   * @return the Set of Cubes
   */
  def getCube(pointCube:Set[(DBScanCube, Int)], bounding: Double): Set[(Int,DBScanCube, Int)] = {
    new Cell_3D(pointCube, bounding).getSplits() // doLALALAL
  }
  println("sssss")
}


case class Cell_3D(pointCube: Set[(DBScanCube, Int)], bounding: Double) {
  type CubeWithCount = (DBScanCube, Int)
  def pointsInCube(space: Set[CubeWithCount], cube: DBScanCube): Int = {
    val count: Int = space.view
      .filter({
        case (current, _) => cube.contains(current)
      })
      .foldLeft(0)({
        case (total, (_, currentCubeCount)) => total + currentCubeCount
      })
    count
  }
  def pointsIn: DBScanCube => Int = pointsInCube(pointCube, _: DBScanCube)

  def getMinimalBounding(pointCube: Set[(DBScanCube, Int)]): DBScanCube = {
    val Cube: Set[DBScanCube] = pointCube.map(_._1)
    val x_min: Double = Cube.map(x => x.x).toList.min
    val x_max: Double = Cube.map(x => x.x2).toList.max
    val y_min: Double = Cube.map(x => x.y).toList.min
    val y_max: Double = Cube.map(x => x.y2).toList.max
    val t_min: Double = Cube.map(x => x.t).toList.min
    val t_max: Double = Cube.map(x => x.t2).toList.max
    DBScanCube(x_min, y_min, t_min, x_max, y_max, t_max)
  }
  def getSplits(): Set[(Int,DBScanCube, Int)] ={
    val boundingCube: DBScanCube = getMinimalBounding(pointCube)
    doSplitWithBounding(boundingCube, bounding)
  }
  def doSplitWithBounding(originCube: DBScanCube, bounding: Double): Set[(Int, DBScanCube, Int)] = {
    val x_split = (originCube.x until originCube.x2 by bounding).toList :+ originCube.x2
    val y_split = (originCube.y until originCube.y2 by bounding).toList :+ originCube.y2
    val t_split = (originCube.t until originCube.t2 by bounding).toList :+ originCube.t2
    // n^3 ? high cost of complexity
    val cubes = for {
      x <- x_split.init
      y <- y_split.init
      t <- t_split.init
    } yield DBScanCube(x, y, t, x + bounding, y + bounding, t + bounding)
    val cubetemp = cubes.toSet
    var index = 0
    val pointofcube: Set[(Int,DBScanCube, Int)] = cubetemp.map { cube =>
      index=index+1
      (index ,cube, pointsIn(cube))
    }
    pointofcube
  }
}