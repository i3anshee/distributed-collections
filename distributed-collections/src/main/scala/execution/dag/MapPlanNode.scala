package execution.dag

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

class MapPlanNode[A, B](val closure: (A) => B) extends PlanNode(Set(), Set()) {

}