package tasks.dag

import collection.distributed.api.dag._
/**
 * @author Vojin Jovanovic
 */

class RuntimeDAG extends PlanDAG[RuntimePlanNode, InputRuntimePlanNode] {
  def initialize() = foreach(_.initialize)

}
