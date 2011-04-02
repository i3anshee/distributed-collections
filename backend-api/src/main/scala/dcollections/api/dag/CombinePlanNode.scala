package execution.dag

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

class CombinePlanNode[A,B,K] (val keyFunction: A => K, val op: (B, A) => B) extends PlanNode {

}