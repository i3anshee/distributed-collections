package execution.dag

import mrapi.ClosureMapperAdapter

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

class MapClosurePlanNode[A, B](val closure: (A) => B)  extends MapPlanNode {
  def mapAdapter() = new ClosureMapperAdapter(closure)
}