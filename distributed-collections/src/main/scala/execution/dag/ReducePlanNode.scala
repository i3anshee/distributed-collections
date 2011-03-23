package execution.dag

import mrapi.ReducerAdapter

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

abstract class ReducePlanNode extends PlanNode() {
  def reduceAdapter(): ReducerAdapter
}