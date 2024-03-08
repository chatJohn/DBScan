package org.apache.spark.Scala.DBScanNaive

import scala.annotation.tailrec

object DBScanGraph {

  def apply[T](): DBScanGraph[T] = {
    new DBScanGraph[T](Map[T, Set[T]]())
  }
}


class DBScanGraph[T] private (nodes: Map[T, Set[T]]) extends Serializable{
  /*
  * Add the given vertex `v` to the graph
  * */
  def addVertex(v: T): DBScanGraph[T] = {
    nodes.get(v) match {
      case None => new DBScanGraph(nodes + (v -> Set()))
      case Some(_) => this
    }
  }

  /*
  * Insert an edge from `from` to `to`
  * */

  def insertEdge(from: T, to: T): DBScanGraph[T] ={
    nodes.get(from) match {
      case None => new DBScanGraph[T](nodes + (from -> Set(to)))
      case Some(edges) => new DBScanGraph[T](nodes + (from -> (edges + to)))
    }
  }


  /*
  * build the undirected edge from one to another
  * */
  def connect(one: T, another: T): DBScanGraph[T] = {
    insertEdge(one, another).insertEdge(another, one)
  }


  @tailrec
  private def getAdjacent(tovisit: Set[T], visited: Set[T], adjacent: Set[T]): Set[T] ={
    tovisit.headOption match {
      case Some(current) =>
        nodes.get(current) match {
          case Some(edges) => {
            getAdjacent(edges.diff(visited) ++ tovisit.tail, visited + current, adjacent ++ edges)
          }
          case None => getAdjacent(tovisit.tail, visited, adjacent)
        }
      case None => adjacent
    }
  }

  /*
    * Find all `vertexs` which are reachable from `from`
    * */
  def getConnected(from: T): Set[T] = {
    getAdjacent(Set(from), Set[T](), Set[T]()) - from
  }

}
