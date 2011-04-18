package scala.collection.distributed.api.dag

import scala.collection.distributed.api.{CollectionId, Emitter}

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class GroupByPlanNode[A, B, K](override val id: CollectionId, keyFunction: (A, Emitter[B]) => K) extends PlanNode(id) {

}