package scala.collection.distributed.api.dag

import scala.collection.mutable
import mutable.{ArrayBuffer, Buffer}
import collection.distributed.api.{ReifiedDistCollection, UniqueId, CollectionId}
;


case class CombinePlanNode[A, K, B, C](op: (C, B) => C,
                                       inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer,
                                       outEdges: mutable.LinkedHashMap[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.LinkedHashMap,
                                       uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = new ArrayBuffer, outEdges = new mutable.LinkedHashMap)

  override def nodeType = "CBN"
}