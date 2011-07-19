package scala.collection.distributed.api.dag

import collection.mutable
import mutable.{ArrayBuffer, Buffer}
import collection.distributed.api.{ReifiedDistCollection, UniqueId, CollectionId, Emitter}

case class GroupByPlanNode[T, V, K](keyFunction: (T) => (K, V),
                                    kmf: Manifest[_],
                                    inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer,
                                    outEdges: mutable.LinkedHashMap[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.LinkedHashMap,
                                    uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = new ArrayBuffer, outEdges = new mutable.LinkedHashMap)

  override def nodeType = "GB"
}