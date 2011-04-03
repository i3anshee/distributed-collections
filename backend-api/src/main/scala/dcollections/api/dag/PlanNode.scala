package dcollections.api.dag

import java.util.UUID


/**
 * User: vjovanovic
 * Date: 3/21/11
 */

abstract class PlanNode(val id: UUID = UUID.randomUUID) {
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