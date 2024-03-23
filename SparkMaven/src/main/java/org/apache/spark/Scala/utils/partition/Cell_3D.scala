package org.apache.spark.Scala.utils.partition

import java.io.PrintWriter

import org.apache.spark.Scala.DBScan3DNaive.DBScanCube


object Cell_3D{
  /**
   * Split the giving Space into some Cubes
   * @return the Set of Cubes
   */
  def getCube(pointCube:Set[(DBScanCube, Int)],x_bounding: Double,y_bounding: Double,t_bounding: Double): Set[(Int,DBScanCube, Int)] = {
    new Cell_3D(pointCube, x_bounding,y_bounding,t_bounding).getSplits() // doLALALAL
  }
}


case class Cell_3D(pointCube: Set[(DBScanCube, Int)], x_bounding: Double,y_bounding: Double,t_bounding: Double) {
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
    doSplitWithBounding(boundingCube, x_bounding,y_bounding,t_bounding)
  }
  def doSplitWithBounding(originCube: DBScanCube, x_bounding: Double,y_bounding: Double,t_bounding: Double): Set[(Int, DBScanCube, Int)] = {
    val x_split = (originCube.x until originCube.x2 by x_bounding).toList :+ originCube.x2
    val y_split = (originCube.y until originCube.y2 by y_bounding).toList :+ originCube.y2
    val t_split = (originCube.t until originCube.t2 by t_bounding).toList :+ originCube.t2
    // n^3 ? high cost of complexity
    val cubes = for {
      x <- x_split.init
      y <- y_split.init
      t <- t_split.init
    } yield DBScanCube(x, y, t, x + x_bounding, y + y_bounding, t + t_bounding)
    val cubetemp = cubes.toSet

    //过滤掉点数为0的Cube
    val cubetemp1: Set[(DBScanCube, Int)] = cubetemp
      .map { cube =>
        (cube, pointsIn(cube))
      }
      .filter { case (_, count) =>
        count != 0
      }
    //给Cube标记索引
    var index = 0
    val pointofcube: Set[(Int,DBScanCube, Int)] = cubetemp1.map { case(cube,count) =>
      index=index+1
      (index ,cube, count)
    }

    val filePath = "D:/START/distribute-ST-cluster/code/Louvain/cube.txt"
    val writer = new PrintWriter(filePath)
    pointofcube.foreach { case (id,cube,count) =>
      writer.println(s"$id\t$count") //"源节点 目标节点 权重"
    }
    writer.close()

    pointofcube
  }
}