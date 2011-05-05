package scala.collection.distributed.api.dag

import collection.distributed.api.{UniqueId, CollectionId, Emitter}
import collection.mutable

case class GroupByPlanNode[A, B, K](keyFunction: (A, Emitter[B]) => K,
                                    kmf: Manifest[_],
                                    inEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                                    outEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                                    uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = mutable.HashMap(), outEdges = mutable.HashMap())
}