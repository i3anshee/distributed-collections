package scala.collection.distributed.api.dag

import scala.collection.distributed.api.{CollectionId, Emitter}

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class CombinePlanNode[A, K, B, C](override val id: CollectionId, keyFunction: (A, Emitter[B]) => K, op: (C, B) => C) extends PlanNode(id) {

}