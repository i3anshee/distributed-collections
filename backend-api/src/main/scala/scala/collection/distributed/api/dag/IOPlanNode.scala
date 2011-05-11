package scala.collection.distributed.api.dag

import collection.distributed.api.ReifiedDistCollection

/**
 * @author Vojin Jovanovic
 */

trait IOPlanNode extends PlanNode {
  val collection: ReifiedDistCollection
}