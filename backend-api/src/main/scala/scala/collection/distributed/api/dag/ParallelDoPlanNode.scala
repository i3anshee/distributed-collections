package scala.collection.distributed.api.dag

import scala.collection.distributed.api.{DistContext, CollectionId, Emitter}

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class ParallelDoPlanNode[A, B](override val id: CollectionId, parOperation: (A, Emitter[B], DistContext) => Unit) extends PlanNode(id) {

}