package scala.collection.distributed.api.dag

import collection.distributed.api.{ReifiedDistCollection, UniqueId}
import collection.{immutable, mutable}
import mutable.ArrayBuffer
;


case class CombinePlanNode[T1, T2 >: T1](op: Iterable[T1] => T2 ,
                                       inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer,
                                       outEdges: mutable.LinkedHashMap[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.LinkedHashMap,
                                       uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = new ArrayBuffer, outEdges = new mutable.LinkedHashMap)

  override def nodeType = "CBN"
}