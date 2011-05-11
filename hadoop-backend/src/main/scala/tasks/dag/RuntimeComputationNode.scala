package tasks.dag

import collection.distributed.api.dag.{PlanNode, IOPlanNode}

/**
 * @author Vojin Jovanovic
 */

class RuntimeComputationNode(val node: PlanNode) extends RuntimePlanNode {

  def copyUnconnected() = new RuntimeComputationNode(node)



}