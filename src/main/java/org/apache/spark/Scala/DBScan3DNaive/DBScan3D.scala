package org.apache.spark.Scala.DBScan3DNaive

import org.apache.spark.Scala.DBScan3DNaive.DBScanLabeledPoint_3D.Flag
import org.apache.spark.Scala.utils.partition.EvenSplitPartition_3D
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector


object DBScan3D{
  def train(data: RDD[Vector],
            distanceEps: Double,
            timeEps: Double,
            minPoints: Int,
            maxPointsPerPartitions: Int,
            x_bounding: Double,
            y_bounding: Double,
            t_bounding: Double
           ): DBScan3D = {
    new DBScan3D(distanceEps, timeEps, minPoints, maxPointsPerPartitions, x_bounding, y_bounding, t_bounding, null, null).train(data)
  }
}

class DBScan3D private(val distanceEps: Double,
                       val timeEps: Double,
                       val minPoints: Int,
                       val maxPointsPerPartition: Int,
                       val x_bounding: Double,
                       val y_bounding: Double,
                       val t_bounding: Double,
                       @transient val partitions: List[(Int, DBScanCube)],
                       @transient private val labeledPartitionedPoints: RDD[(Int, DBScanLabeledPoint_3D)])
extends Serializable with  Logging{
  type Margin = (DBScanCube, DBScanCube, DBScanCube)
  type ClusterID = (Int, Int)
  def minimumRectangleSize: Double = 2 * distanceEps
  def minimumHigh: Double = 2 * timeEps
  def labeledPoints: RDD[DBScanLabeledPoint_3D] = {
    labeledPartitionedPoints.values // all labeled points in working space after implementing the DBScan
  }
  def findAdjacencies(partition: Iterable[(Int, DBScanLabeledPoint_3D)]): Set[((Int, Int), (Int, Int))] = {
    val zero = (Map[DBScanPoint_3D, ClusterID](), Set[(ClusterID, ClusterID)]())

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
  def isInnerPoint(entry: (Int, DBScanLabeledPoint_3D), margins: List[(Margin, Int)]): Boolean = {
    entry match {
      case (partition, point) => {
        val ((inner, main, outer), _) = margins.filter({
          case (_, id) => id == partition
        }).head
        inner.almostContains(point)
      }
    }
  }
  def shiftIfNegative(p: Double, minimum: Double): Double= {
    if(p < 0) {
      p - minimum
    } else {
      p
    }
  }
  def corner(p: Double, minimum: Double): Double = {
    (shiftIfNegative(p, minimum) / minimum).intValue * minimum
  }
  private def toMinimumBoundingCube(vector: Vector): DBScanCube = {
    val point: DBScanPoint_3D = DBScanPoint_3D(vector) // object DBScanPoint
    val x = corner(point.distanceX, minimumRectangleSize)
    val y = corner(point.distanceY, minimumRectangleSize)
    val time = corner(point.timeDimension, minimumHigh)
    DBScanCube(x, y, time, x + minimumRectangleSize, y + minimumRectangleSize, time + minimumHigh)

  }
  private def train(data: RDD[Vector]): DBScan3D = {
    println("About to train")
    val minimumCubeWithCount: Set[(DBScanCube, Int)] = data
      .map(x => {
        toMinimumBoundingCube(x) // give every point the minimum bounding rectangle
      })
      .map(x => (x, 1))
      .aggregateByKey(0)(_ + _, _ + _) // 先同一个RDD中相同Rectangle数据点相加，然后所有RDD中相同的Rectangle的数据点相加
      .collect()
      .toSet // 构建全局数据点的立方体

    val localPartitions: List[(DBScanCube, Int)]
      = EvenSplitPartition_3D.partition(minimumCubeWithCount,
        maxPointsPerPartition,
        minimumRectangleSize,
        minimumHigh)
    println(s"local partition size: ${localPartitions.size}")

    val localCube: List[((DBScanCube, DBScanCube, DBScanCube), Int)] = localPartitions.map({
      case (p, _) => (p.shrink(distanceEps,timeEps), p, p.shrink(-distanceEps,-timeEps))
    }).zipWithIndex
    val margins: Broadcast[List[((DBScanCube, DBScanCube, DBScanCube), Int)]] = data.context.broadcast(localCube)

    // BaseLine method
    val duplicated: RDD[(Int, DBScanPoint_3D)] = for {
      point <- data.map(new DBScanPoint_3D(_))
      ((inner, main, outer), id) <- margins.value
      if outer.contains(point)
    } yield (id, point) // the point in the partition with id

    val numberOfPartitions: Int = localPartitions.size
    println("perform local DBScan")
    val clustered: RDD[(Int, DBScanLabeledPoint_3D)] = duplicated
      .groupByKey(numberOfPartitions) // param: numPartitions
      .flatMapValues((points: Iterable[DBScanPoint_3D]) => {
        println("About to begin the local DBScan")
        new LocalDBScan_3D(distanceEps, timeEps, minPoints).fit(points)
      }) // different partition has different clustering

    println("find all candidate points for merging clusters and group them => inner margin & outer margin")
    val marginPoints: RDD[(Int, Iterable[(Int, DBScanLabeledPoint_3D)])] = clustered.flatMap({
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
    val adjacenciesGraph = adjacencies.foldLeft(DBScanGraph_3D[ClusterID]())({
      case (graph, (from, to)) => graph.connect(from, to)
    })

    println("About to find all cluster ids")

    val localClusterIds = clustered.filter({
      case (_, points) => points.flag != Flag.Noise
    }).mapValues((x: DBScanLabeledPoint_3D) => x.cluster)
      .distinct()
      .collect()
      .toList

    // assign a global cluster id to all clusters, where connected clusters get the same id

    val (total, clusterIdToGlobalId) = localClusterIds.foldLeft((0, Map[ClusterID, Int]()))({
      case ((id, map), clusterId) => {
        map.get(clusterId) match {
          case None => {
            val nextId = id + 1
            val connectedClusters = adjacenciesGraph.getConnected(clusterId) + clusterId
            println(s"Connected cluster: $connectedClusters")

            val toadd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toadd)
          }
          case Some(_) => (id, map)
        }
      }
    })

    println("Global Clusters")
    clusterIdToGlobalId.foreach(x => println(x.toString()))
    println(s"Total Clusters: ${localClusterIds.size}, Unique: $total")

    val clusterIds = data.context.broadcast(clusterIdToGlobalId)

    println("About to relabel inner points")
    val labeledInner: RDD[(Int, DBScanLabeledPoint_3D)] = clustered.filter(isInnerPoint(_, margins.value))
      .map({
        case (partition, point) => {
          if (point.flag != Flag.Noise) {
            point.cluster = clusterIds.value((partition, point.cluster))
          }
          (partition, point)
        }
      })


    println("About to relabel outer points")
    val labeledOuter =
      marginPoints.flatMapValues(partition => {
        partition.foldLeft(Map[DBScanPoint_3D, DBScanLabeledPoint_3D]())({
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

    val finalPartitions = localCube.map {
      case ((_, p, _), index) => (index, p)
    }

    println("Done")
    new DBScan3D(
      distanceEps,
      timeEps,
      minPoints,
      maxPointsPerPartition,
      x_bounding,
      y_bounding,
      t_bounding,
      finalPartitions,
      labeledInner.union(labeledOuter))
  }
}
