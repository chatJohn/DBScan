package org.apache.spark.Scala.DBScan3DNaive

import org.apache.spark.Scala.DBScan3DNaive.DBScanLabeledPoint_3D.Flag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.Scala.utils.partition.CubeSplitPartition_3D
import org.apache.spark.Scala.utils.sample.Sample

object DBScan3D_CubeSplit{
  def train(data: RDD[Vector],
            distanceEps: Double,
            timeEps: Double,
            minPoints: Int,
            maxPointsPerPartitions: Int,
            x_bounding: Double,
            y_bounding: Double,
            t_bounding: Double
           ): DBScan3D_CubeSplit = {
    new DBScan3D_CubeSplit(distanceEps, timeEps, minPoints, maxPointsPerPartitions,x_bounding,y_bounding,t_bounding,null, null).train(data)
  }
}

class DBScan3D_CubeSplit private(val distanceEps: Double,
                                 val timeEps: Double,
                                 val minPoints: Int,
                                 val maxPointsPerPartition: Int,
                                 val x_bounding: Double,
                                 val y_bounding: Double,
                                 val t_bounding: Double,
                                 @transient val partitions: List[(Int, DBScanCube)],
                                 @transient private val labeledPartitionedPoints: RDD[(Int, DBScanLabeledPoint_3D)])
  extends Serializable with  Logging{
  type Margin = Set[(DBScanCube, DBScanCube, DBScanCube)]
  type ClusterID = (Int, Int)
  def minimumRectangleSize: Double = 2 * distanceEps
  def minimumHigh: Double = 2 * timeEps
  def labeledPoints: (RDD[DBScanLabeledPoint_3D], Long) = {
    (labeledPartitionedPoints.values, labeledPartitionedPoints.count()) // all labeled points in working space after implementing the DBScan
  }
  def findAdjacencies(partitions: Iterable[(Int, DBScanLabeledPoint_3D)]): Set[((Int, Int), (Int, Int))] = {

    val zero = (Map[DBScanPoint_3D, ClusterID](), Set[(ClusterID, ClusterID)]())
    val partitionsMap: Map[Int, DBScanLabeledPoint_3D] = partitions.toMap
    val (seen, adjacencies) = partitions.foldLeft(zero)({
      case ((seen, adajacencies), (partition, point)) => {
        // noise points are not relevant to any adajacencies
        if (point.flag == Flag.Noise) {
          (seen, adajacencies)
        } else if (point.flag == Flag.Core){
          val clusterId = (partition, point.cluster)

          seen.get(point) match {
            case None => (seen + (point -> clusterId), adajacencies)
            case Some(preClusterId) => (seen, adajacencies + ((preClusterId, clusterId)))
          }
        }else{
          val clusterId = (partition, point.cluster)
          seen.get(point) match {
            case Some(preClusterId) =>{
              if(partitionsMap(preClusterId._1).flag == Flag.Core){
                (seen, adajacencies + ((preClusterId, clusterId)))
              }else{
                (seen, adajacencies)
              }
            }
          }
        }
      }
    })
    adjacencies
  }

  def isInnerPoint(entry: (Int, DBScanLabeledPoint_3D), margins: List[(Margin, Int)]): Boolean = {
    entry match {
      case (partition, point) =>
        margins.exists {
          case (cubeSet, id) => id == partition && cubeSet.exists {
            case (inner, _, _) => inner.almostContains(point)
          }
        }
    }
  }

  private def train(data: RDD[Vector]): DBScan3D_CubeSplit = {
    println("The Begin of Program: the count of ds is: " + data.count())
    val points: Array[DBScanPoint_3D] = data
      .map(x => {
        DBScanPoint_3D(x) // give every point the minimum bounding rectangle
      })
      .collect()
    val samplePoints: RDD[DBScanPoint_3D] = Sample.sample(data, sampleRate = 0.1)
    println("Sample Done Size: " + samplePoints.count()) // √
    // New method
    val localPartitions: List[Set[DBScanCube]]
    = CubeSplitPartition_3D.getPartition(samplePoints.collect(),
      x_bounding,
      y_bounding,
      t_bounding,
      maxPointsPerPartition
    )

    var localCubeTemp: List[Set[(DBScanCube, DBScanCube, DBScanCube)]] = List()
    for(cubeSet <- localPartitions){
      var cubeShrink : Set[(DBScanCube, DBScanCube, DBScanCube)]= Set()
      for(p <- cubeSet){
        cubeShrink += ((p.shrink(distanceEps,timeEps), p, p.shrink(-distanceEps,-timeEps)))
      }
      localCubeTemp = cubeShrink :: localCubeTemp
    }
    val localCube: List[(Set[(DBScanCube, DBScanCube, DBScanCube)], Int)] = localCubeTemp.zipWithIndex
    val margins: Broadcast[List[(Set[(DBScanCube, DBScanCube, DBScanCube)], Int)]] = data.context.broadcast(localCube)

    val duplicated: RDD[(Int, DBScanPoint_3D)] = for {
      point <- data.map(new DBScanPoint_3D(_))
      (cubeSet, id)<- margins.value
      (inner, main, outer) <- cubeSet
      if outer.contains(point)
    } yield (id, point) // the point in the partition with id, about all points and some points in outer margin

    val duplicatedCount: Long = duplicated.count()
    println("Total count of duplicated elements: " + duplicatedCount)

    val numberOfPartitions: Int = localPartitions.size
    println("perform local DBScan")
    val clustered: RDD[(Int, DBScanLabeledPoint_3D)] = duplicated
      .groupByKey(numberOfPartitions) // param: numPartitions, parallel number
      .flatMapValues((points: Iterable[DBScanPoint_3D]) => {
        println("About to begin the local DBScan")
        new LocalDBScan_3D(distanceEps, timeEps, minPoints).fit(points)
      }) // different partition has different clustering

    println("find all candidate points for merging clusters and group them => inner margin & outer margin")

    val marginPoints: RDD[(Int, Iterable[(Int, DBScanLabeledPoint_3D)])] = clustered.flatMap({
      case (partition, point) => {
        margins.value.map {
          case (cubeSet, id) =>
            val filteredCubeSet = cubeSet.filter {
              case (inner, main, outer) => main.contains(point) && !inner.almostContains(point)
            }
            (filteredCubeSet, id)
        }.filter {
          case (filteredCubeSet, _) => filteredCubeSet.nonEmpty
        }.map({
          case (_, newPartition) => (newPartition, (partition, point))
        })
      }
    }).groupByKey()

    println("find all candidate points Done!")

    println("About to find adjacencies")
    val adjacencies: Array[((Int, Int), (Int, Int))] = marginPoints.flatMapValues(x => findAdjacencies(x)).values.collect()
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
            val connectedClusters: Set[(Int, Int)] = adjacenciesGraph.getConnected(clusterId) + clusterId
            println(s"Connected cluster: $connectedClusters")
            val toAdd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toAdd)
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

    println("Done")
    new DBScan3D_CubeSplit(
      distanceEps,
      timeEps,
      minPoints,
      maxPointsPerPartition,
      x_bounding,
      y_bounding,
      t_bounding,
      null,
      labeledInner.union(labeledOuter))
  }
}