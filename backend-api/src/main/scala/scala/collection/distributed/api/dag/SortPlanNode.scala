package scala.collection.distributed.api.dag

import collection.distributed.api.{UniqueId, CollectionId}
import collection.mutable

case class SortPlanNode[T, S](by: (T) => Ordered[S],
                              smf: Manifest[_],
                              inEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                              outEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                              uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = mutable.HashMap(), outEdges = mutable.HashMap())
}