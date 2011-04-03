package dcollections.api.dag

import dcollections.api.Emitter

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class ParallelDoPlanNode[A, B](parOperation: (A, Emitter[B]) => Unit) extends PlanNode {

}