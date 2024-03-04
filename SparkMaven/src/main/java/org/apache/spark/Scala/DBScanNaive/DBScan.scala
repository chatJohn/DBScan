package org.apache.spark.Scala.DBScanNaive


import orestes.bloomfilter.CountingBloomFilter
import org.apache.spark.Scala.DBScanBloom_BitMap.{Cell, CellBloomFilter, Rectangle}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.Scala.DBScanNaive.DBScanLabeledPoint.Flag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

import scala.collection.mutable.ArrayBuffer

object DBScan {
  /*
  * Train a DBScan Model using the given set of parameters
  *
  * @param data : training points stored as `RDD[Vector]`
  * only the fist two points of the vector are taken into consideration
  * @param eps the maximum distance between two points for them to be considered as a part
  * of the same region
  *
  * @param minPoint the minimum number of points required to be a core
  *
  * @param maxPointsPerPartition the largest number of points in a single partition
  *
  *
  * */
  var sc: SparkContext = null
  def train(data: RDD[Vector],
            eps: Double,
            minPoints: Int,
            maxPointsPerPartition: Int,
            x_bounding: Double,
            y_bouding: Double,
            sc: SparkContext): DBScan = {
    this.sc = sc
    new DBScan(eps, minPoints, maxPointsPerPartition, x_bounding, y_bouding, null, null).train(data)
  }
}


class DBScan private(val eps: Double,
                     val minPoints: Int,
                     val maxPointsPerPartition: Int,
                     val x_bounding: Double,
                     val y_bouding: Double,
                     @transient val partitions: List[(Int, DBScanRectangle)],
                     @transient private val labeledPartitionedPoints: RDD[(Int, DBScanLabeledPoint)])
  extends Serializable  with Logging{

  type Margins = (DBScanRectangle, DBScanRectangle, DBScanRectangle) // inner, main, outer
  type ClusterId = (Int, Int) //
  def minimumRectangleSize = 2 * eps
  def labeledPoints: RDD[DBScanLabeledPoint] = {
    labeledPartitionedPoints.values // all labeled points in working space after implementing the DBScan
  }

  def findAdjacencies(partition: Iterable[(Int, DBScanLabeledPoint)]): Set[((Int, Int), (Int, Int))] = {
    val zero = (Map[DBScanPoint, ClusterId](), Set[(ClusterId, ClusterId)]())

    val (seen, adjacencies) = partition.foldLeft(zero)({
      case ((seen, adajacencies), (partition, point)) => {
        // noise points are not relevant to any adajacencies
        if (point.flag == Flag.Noise) {
          (seen, adajacencies)
        } else {
          val clusterId = (partition, point.cluster)

          seen.get(point) match {
            case None => (seen + (point -> clusterId), adajacencies)
            case Some(preClusterId) => (seen, adajacencies + ((preClusterId, clusterId)))
          }
        }
      }
    })
    adjacencies
  }

  def isInnerPoint(entry: (Int, DBScanLabeledPoint), margins: List[(Margins, Int)]): Boolean = {
    entry match {
      case (partition, point) => {
        val ((inner, main, outer), _) = margins.filter({
          case (_, id) => id == partition
        }).head
        inner.almostContains(point)
      }
    }
  }
  private def point2Rectangle(point: DBScanPoint, eps: Double): Rectangle = {
    Rectangle(point.x - eps, point.y - eps, point.x + eps, point.y)
  }
  /**
   * this function for decreasing the number of points duplicated
   * Notes: this function can be optimal
   * @param vectors
   * @param margins
   * @return
   */
  private def GetPointWithId(vectors: RDD[Vector], margins: Broadcast[List[((DBScanRectangle, DBScanRectangle, DBScanRectangle), Int)]]): List[(Int, DBScanPoint)] = {
    val allCells: Set[Rectangle] = new Cell(vectors, x_bounding, y_bouding, eps).getCell(data = vectors)
    val cellBloomFilter: CellBloomFilter = new CellBloomFilter(data = vectors, allCell = allCells)
    val countBloomFilter: CountingBloomFilter[String] = cellBloomFilter.buildBloomFilter()
    val bitMap: ArrayBuffer[Int] = cellBloomFilter.getBitMap(allCell = allCells.zipWithIndex, countingBloomFilter = countBloomFilter, eps = eps, maxPoint = minPoints)
    var res: List[(Int, DBScanPoint)] = List[(Int, DBScanPoint)]()
    for(point <- vectors){
      val dBScanPoint: DBScanPoint = new DBScanPoint(point)
      val pointRec = point2Rectangle(dBScanPoint, eps)
      for(((_, _, outer), id) <- margins.value) {
        if(outer.contains(dBScanPoint)){ // this point should be outer rectangle first
          val rectangle: Rectangle =  Rectangle(outer.x, outer.y, outer.x2, outer.y2)
          for (((_, _, outer1), id1) <- margins.value){
            val rectangle1: Rectangle = Rectangle(outer1.x, outer1.y, outer1.x2, outer1.y2)
            if(rectangle1.hasUnit(pointRec)){ // the rectangle which developed by this point should have unit area with another rectangle
              val UnitRectangle: Rectangle = rectangle1.getUnit(pointRec) // get the Unit rectangle
              for ((cell, cellId) <- allCells.zipWithIndex){
                if(UnitRectangle.hasUnit(cell) && bitMap(cellId) == 1){
                  res = res:+(id1, dBScanPoint)
                }
              }
            }
          }
        }
      }
    }
    res
  }

  private def train(vectors: RDD[Vector]) :DBScan = {
    // generate the smallest rectangles that the space and
    // count the number of points in each one of them

    println("About to train")
    // 构建全局的bitMap, 和CountingBloomFilter



    val minimumRectangleWithCount = vectors
      .map(x => {
        toMinimumBoundingRectangle(x) // give every point the minimum bounding rectangle
      })
      .map(x => (x, 1))
      .aggregateByKey(0)(_ + _, _ + _) // 先同一个RDD中相同Rectangle数据点相加，然后所有RDD中相同的Rectangle的数据点相加
      .collect()
      .toSet // 构建全局数据点的最小约束矩形 ?


    val totalSum: Int = minimumRectangleWithCount.map { case (_, count) => count }.sum
    println(totalSum)
    println("find the best partition for the data space")
    val localPartitions: List[(DBScanRectangle, Int)]
    = EvenSplitPartitioner.partition( minimumRectangleWithCount,
      maxPointsPerPartition,
      minimumRectangleSize,eps)

    println(s"Found partitions: $localPartitions")
    localPartitions.foreach(p => println(p.toString()))

    // grow partitions to include eps
    val localMargins = localPartitions.map({
      case (p, _) => (p.shrink(eps), p, p.shrink(-eps))
    }).zipWithIndex

    val margins: Broadcast[List[((DBScanRectangle, DBScanRectangle, DBScanRectangle), Int)]] = vectors.context.broadcast(localMargins) // optimations place?

  // assign each point to its proper partition
      val duplicated: RDD[(Int, DBScanPoint)] = for {
        point <- vectors.map(new DBScanPoint(_))
        ((inner, main, outer), id) <- margins.value // i <- limit, and j <- limits for every i
  //      if outer.contains(point) // optimation place?
        // ==> the change version
        if outer.contains(point)
      } yield (id, point) // the point in the partition with id

    // first to filter the outer point
    //    vectors.map(x => new DBScanPoint(x)).filter()

//    val duplicated: RDD[(Int, DBScanPoint)] = DBScan.sc.parallelize(GetPointWithId(vectors, margins))

    val numberOfPartitions: Int = localPartitions.size
    println(s"Local partitions size: $numberOfPartitions")
    println("perform local DBScan")


    val clustered: RDD[(Int, DBScanLabeledPoint)] = duplicated
      .groupByKey(numberOfPartitions) // param: numPartitions
      .flatMapValues((points: Iterable[DBScanPoint]) => {
        println("About to begin the local DBScan")
        new LocalDBScanNaive(eps, minPoints).fit(points)
      }) // different partition has different clustering

    println("find all candidate points for merging clusters and group them => inner margin & outer margin")
    val marginPoints: RDD[(Int, Iterable[(Int, DBScanLabeledPoint)])] = clustered.flatMap({
      case (partition, point) => {
        margins.value
          .filter({
            case ((inner, main, outer), _) => main.contains(point) && !inner.almostContains(point)
            /*
            * not in inner rectangle not including the border of the inner rectangle
            * */
          })
          .map({
            case (_, newPartition) => (newPartition, (partition, point))
          })
      }
    }).groupByKey()
    println("find all candidate points Done!")


    println("About to find adjacencies")

    val adjacencies = marginPoints.flatMapValues(x => findAdjacencies(x)).values.collect()

    // generated adjacency graph
    val adjacenciesGraph = adjacencies.foldLeft(DBScanGraph[ClusterId]())({
      case (graph, (from, to)) => graph.connect(from, to)
    })

    println("About to find all cluster ids")

    // find all cluster id
    val localClusterIds = clustered.filter({
      case (_, points) => points.flag != Flag.Noise
    }).mapValues((x: DBScanLabeledPoint) => x.cluster)
      .distinct()
      .collect()
      .toList

    // assign a global cluster id to all clusters, where connected clusters get the same id

    val (total, clusterIdToGlobalId) = localClusterIds.foldLeft((0, Map[ClusterId, Int]()))({
      case ((id, map), clusterId) => {
        map.get(clusterId) match {
          case None => {
            val nextId = id + 1
            val connectedClusters = adjacenciesGraph.getConnected(clusterId) + clusterId
            println(s"Connected cluster: $connectedClusters")

            val toadd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toadd)
          }
          case Some(x) => (id, map)
        }
      }
    })


    println("Global Clusters")
    clusterIdToGlobalId.foreach(x => println(x.toString()))
    println(s"Total Clusters: ${localClusterIds.size}, Unique: $total")


    val clusterIds = vectors.context.broadcast(clusterIdToGlobalId)

    println("About to relabel inner points")

    // relabel non-duplicated points

    val labeledInner: RDD[(Int, DBScanLabeledPoint)] = clustered.filter(isInnerPoint(_, margins.value))
      .map({
        case (partition, point) => {
          if (point.flag != Flag.Noise) {
            point.cluster = clusterIds.value((partition, point.cluster))
          }
          (partition, point)
        }
      })
    //    println("Relabel inner points Done! And the details show below")
    //    labeledInner.foreach(println)

    println("About to relabel outer points")
    val labeledOuter =
      marginPoints.flatMapValues(partition => {
        partition.foldLeft(Map[DBScanPoint, DBScanLabeledPoint]())({
          case (all, (partition, point)) =>

            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }

            all.get(point) match {
              case None => all + (point -> point)
              case Some(prev) => {
                // override previous entry unless new entry is noise
                if (point.flag != Flag.Noise) {
                  prev.flag = point.flag
                  prev.cluster = point.cluster
                }
                all
              }
            }

        }).values
      })
    val finalPartitions = localMargins.map {
      case ((_, p, _), index) => (index, p)
    }

    println("Done")

    new DBScan(
      eps,
      minPoints,
      maxPointsPerPartition,
      x_bounding,
      y_bouding,
      finalPartitions,
      labeledInner.union(labeledOuter))
  }




  /**
   * this method can get the minimum bounding rectangle, and it can show the lower bound and upper bound
   * @param p
   * @return
   */
  def shiftIfNegative(p: Double): Double= {
    if(p < 0) {
      p - minimumRectangleSize
    } else {
      p
    }
  }
  def corner(p: Double): Double = {
    (shiftIfNegative(p) / minimumRectangleSize).intValue * minimumRectangleSize
  }

  private def toMinimumBoundingRectangle(vector: Vector): DBScanRectangle = {
    val point: DBScanPoint = DBScanPoint(vector) // object DBScanPoint
    val x = corner(point.x)
    val y = corner(point.y)
    DBScanRectangle(x, y, x + minimumRectangleSize, y + minimumRectangleSize)
    /*
     minimumRectangleSize is 2 * eps
    */
  }



  /**
   * Find the appropriate label to the given `vector`
   *
   * This method is not yet implemented
   */
  def predict(vector: Vector): DBScanLabeledPoint = {
    throw new NotImplementedError
  }

}