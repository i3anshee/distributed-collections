package scala.collection.distributed.api.dag

import collection.immutable.GenSeq
import collection.distributed.api.{UniqueId, DistContext, UntypedEmitter, CollectionId}
import collection.mutable

case class DistDoPlanNode[T](distOP: (T, UntypedEmitter, DistContext) => Unit,
                             outputs: GenSeq[CollectionId],
                             inEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                             outEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                             uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = mutable.HashMap(), outEdges = mutable.HashMap())
}
