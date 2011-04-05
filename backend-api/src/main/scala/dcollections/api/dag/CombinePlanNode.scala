package dcollections.api.dag

import dcollections.api.CollectionId

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class CombinePlanNode[A, B, K](override val id: CollectionId, keyFunction: A => K, op: (B, A) => B) extends PlanNode(id) {

}