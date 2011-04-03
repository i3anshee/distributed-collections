package dcollections.api.dag

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

case class CombinePlanNode[A, B, K](keyFunction: A => K, op: (B, A) => B) extends PlanNode {

}