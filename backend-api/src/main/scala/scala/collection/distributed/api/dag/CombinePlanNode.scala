package scala.collection.distributed.api.dag

import scala.collection.mutable
import collection.distributed.api.{UniqueId, CollectionId}
;


case class CombinePlanNode[A, K, B, C](op: (C, B) => C,
                                       inEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                                       outEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                                       uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = mutable.HashMap(), outEdges = mutable.HashMap())
}