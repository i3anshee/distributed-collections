package scala.collection.distributed.api.dag

import scala.collection.distributed.api.CollectionId

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

case class InputPlanNode(override val id: CollectionId) extends PlanNode(id) {

}