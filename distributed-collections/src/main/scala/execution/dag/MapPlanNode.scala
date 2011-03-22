package execution.dag

import mrapi.MapperAdapter

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

abstract class MapPlanNode extends PlanNode(Set(), Set()) {
  def mapAdapter():MapperAdapter
}