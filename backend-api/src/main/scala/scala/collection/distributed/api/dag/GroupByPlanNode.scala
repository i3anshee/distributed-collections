package scala.collection.distributed.api.dag

import collection.mutable
import mutable.{ArrayBuffer, Buffer}
import collection.distributed.api.{ReifiedDistCollection, UniqueId, CollectionId, Emitter}

case class GroupByPlanNode[A, B, K](keyFunction: (A, Emitter[B]) => K,
                                    kmf: Manifest[_],
                                    inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer,
                                    outEdges: mutable.Map[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.HashMap,
                                    uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = new ArrayBuffer, outEdges = new mutable.HashMap)

  override def nodeType = "GB"
}