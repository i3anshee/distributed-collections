package dcollections.api.dag

import dcollections.api.CollectionId
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