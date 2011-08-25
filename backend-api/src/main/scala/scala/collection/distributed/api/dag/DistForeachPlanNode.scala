package scala.collection.distributed.api.dag

import collection.mutable
import collection.distributed.api._
import mutable.ArrayBuffer

case class DistForeachPlanNode[T, U](distOP: (T) => U,
                             inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer,
                             outEdges: mutable.LinkedHashMap[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.LinkedHashMap,
                             uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected = copy(inEdges = new ArrayBuffer, outEdges = new mutable.LinkedHashMap)

  override def nodeType = "FOREACH"
}