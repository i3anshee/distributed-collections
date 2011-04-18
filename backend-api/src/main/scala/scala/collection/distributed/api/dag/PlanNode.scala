package scala.collection.distributed.api.dag

import scala.collection.distributed.api.CollectionId
import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

abstract class PlanNode(val id: CollectionId) {
  var inEdges: Set[PlanNode] = Set()
  var outEdges: Set[PlanNode] = Set()

  def addInEdge(planNode: PlanNode): PlanNode = {
    inEdges += planNode
    this
  }

  def addOutEdge(planNode: PlanNode): PlanNode = {
    outEdges += planNode
    this
  }
}