package dcollections.api.dag

import dcollections.api.Emitter
/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class GroupByPlanNode[A, B, K](keyFunction: (A, Emitter[B]) => K) extends PlanNode {

}