package dcollections.api.dag

import dcollections.api.{CollectionId, Emitter}

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class CombinePlanNode[A, K, B, C](override val id: CollectionId, keyFunction: (A, Emitter[B]) => K, op: (C, B) => C) extends PlanNode(id) {

}