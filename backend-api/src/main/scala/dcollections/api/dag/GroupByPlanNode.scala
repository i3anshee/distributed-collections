package dcollections.api.dag

import _root_.execution.dag.PlanNode

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

class GroupByPlanNode[A, K](val keyFunction: A => K) extends PlanNode {

}