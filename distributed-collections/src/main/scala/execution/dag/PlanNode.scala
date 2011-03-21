package execution.dag

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

class PlanNode(var inEdges: Set[PlanNode], var outEdges: Set[PlanNode]) {

  def addInEdge(planNode: PlanNode) = {
    inEdges += planNode
  }

  def addOutEdge(planNode: PlanNode) = {
    outEdges += planNode
  }
}