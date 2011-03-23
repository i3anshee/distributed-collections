package execution.dag

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

class PlanNode(var inEdges: Set[PlanNode] = Set(), var outEdges: Set[PlanNode] = Set()) {

  def addInEdge(planNode: PlanNode): PlanNode = {
    inEdges += planNode
    this
  }

  def addOutEdge(planNode: PlanNode): PlanNode = {
    outEdges += planNode
    this
  }
}