package dcollections.api.dag

import dcollections.api.{DistContext, CollectionId, Emitter}

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class ParallelDoPlanNode[A, B](override val id: CollectionId, parOperation: (A, Emitter[B], DistContext) => Unit) extends PlanNode(id) {

}