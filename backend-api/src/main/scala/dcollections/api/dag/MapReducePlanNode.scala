package execution.dag

import mrapi.ToOneKeyMapperAdapter

/**
 * User: vjovanovic
 * Date: 3/23/11
 */

class MapReducePlanNode extends MapPlanNode {
  def mapAdapter() = new ToOneKeyMapperAdapter
}