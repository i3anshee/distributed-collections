package execution.dag

import dcollections.api.Emitter

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

class ParallelDoPlanNode[A, B](val parOperation: (A, Emitter[B]) => Unit) extends PlanNode {

}