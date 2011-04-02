package execution.dag

import mrapi.ReduceSetAdapter

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

class ReduceSetPlanNode extends ReducePlanNode {
  def reduceAdapter() = new ReduceSetAdapter()
}