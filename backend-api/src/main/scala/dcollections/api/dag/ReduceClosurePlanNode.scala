package execution.dag

import mrapi.ReduceClosureAdapter

/**
 * User: vjovanovic
 * Date: 3/23/11
 */

class ReduceClosurePlanNode[A, B](val op: (A, B) => B) extends ReducePlanNode {
  def reduceAdapter() = new ReduceClosureAdapter[A, B](op)
}