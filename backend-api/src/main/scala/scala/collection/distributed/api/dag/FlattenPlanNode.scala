package scala.collection.distributed.api.dag

import collection.mutable
import collection.distributed.api.{ReifiedDistCollection, UniqueId, CollectionId}
import mutable.{Buffer, ArrayBuffer}

case class FlattenPlanNode(collections: Traversable[CollectionId],
                           mf: Manifest[_],
                           inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer,
                           outEdges: mutable.Map[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.HashMap,
                           uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = new ArrayBuffer, outEdges = new mutable.HashMap)

  override def nodeType = "FLT"

}