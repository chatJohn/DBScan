package org.apache.spark.Scala.DBScanNaive

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.internal.Logging
import org.apache.spark.Scala.DBScanNaive.DBScanLabeledPoint.Flag
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable

class LocalDBScanNaive(eps1: Double, eps2:Double, minPoints: Int) extends Logging{
  val minDistanceSquared: Double = eps1 * eps1


//  def samplePoint = Array(new DBScanLabeledPoint(Vectors.dense(Array(0D, 0D))))


  def fit(points: Iterable[DBScanPoint]): Iterable[DBScanLabeledPoint] = {
    println(s"About to start fitting")

    val labeledPoints = points.map(new DBScanLabeledPoint(_)).toArray

    val totoalClusters = labeledPoints.foldLeft(DBScanLabeledPoint.Unknown)(
      (cluster, point) => {
        if (!point.visited) {
          point.visited = true
          val neighbors = findNeighbors(point, labeledPoints)
          if (neighbors.size < minPoints) {
            point.flag = Flag.Noise // ? loose strict
            cluster
          } else {
            println("About to expand the cluster")
            expandCluster(point, neighbors, labeledPoints, cluster + 1) // new core, new cluster
            cluster + 1
          }

        } else {
          cluster
        }
      }
    )
    println(s"found: $totoalClusters clusters")
    labeledPoints
  }

  private def findNeighbors(point: DBScanPoint,
                            all: Array[DBScanLabeledPoint])
  : Iterable[DBScanLabeledPoint] = {
    all.view.filter(other => {
      point.distanceSquared(other) <= minDistanceSquared && point.timedistance(other) <= eps2
    })
  }


  def expandCluster(point: DBScanLabeledPoint,
                    neighbors: Iterable[DBScanLabeledPoint],
                    all: Array[DBScanLabeledPoint],
                    cluster: Int): Unit = {
    point.flag = Flag.Core
    point.cluster = cluster

    var allNeighbors = mutable.Queue(neighbors)
    while(allNeighbors.nonEmpty){
      allNeighbors.dequeue().foreach(neighobr => {
        if(!neighobr.visited){
          neighobr.visited = true
          neighobr.cluster = cluster
          val neighborNeighbors = findNeighbors(neighobr, all)
          if(neighborNeighbors.size >= minPoints){
            neighobr.flag = Flag.Core
            allNeighbors.enqueue(neighborNeighbors)
          }else{
            neighobr.flag = Flag.Border
          }
        }
        else if(neighobr.cluster == DBScanLabeledPoint.Unknown){
          neighobr.cluster = cluster
          neighobr.flag = Flag.Border
        } // point was labeled is Noise before, so the cluster is Unknown, but it can be the Border which matches the other core point
      })
    }
  }
}
