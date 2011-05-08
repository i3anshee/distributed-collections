package scala.collection.distributed.api.dag

import collection.mutable
import collection.distributed.api.{ReifiedDistCollection, UniqueId, CollectionId}
import mutable.{ArrayBuffer, Buffer}

case class SortPlanNode[T, S](by: (T) => Ordered[S],
                              smf: Manifest[_],
                              inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer,
                              outEdges: mutable.Map[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.HashMap,
                              uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = new ArrayBuffer, outEdges = new mutable.HashMap)

  override def nodeType = "SRT"
}