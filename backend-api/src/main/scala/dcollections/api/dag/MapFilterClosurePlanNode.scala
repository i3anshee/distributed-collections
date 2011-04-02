package execution.dag

import mrapi.ClosureFilterAdapter

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

class MapFilterClosurePlanNode[A](predicate: (A => Boolean)) extends MapPlanNode {
  def mapAdapter() = new ClosureFilterAdapter(predicate)
}