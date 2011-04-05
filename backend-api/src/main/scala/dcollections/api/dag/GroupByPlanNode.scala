package dcollections.api.dag

import dcollections.api.{CollectionId, Emitter}

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class GroupByPlanNode[A, B, K](override val id: CollectionId, keyFunction: (A, Emitter[B]) => K) extends PlanNode(id) {

}