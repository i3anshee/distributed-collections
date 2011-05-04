package scala.collection.distributed.api.dag

import collection.distributed.api.{DistContext, Emitter}

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class ParallelDoPlanNode[A, B](parOperation: (A, Emitter[B], DistContext) => Unit) extends PlanNode