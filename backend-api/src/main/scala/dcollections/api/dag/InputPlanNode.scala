package dcollections.api.dag

import dcollections.api.CollectionId

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

case class InputPlanNode(override val id: CollectionId) extends PlanNode(id) {

}