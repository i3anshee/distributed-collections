package scala.collection.distributed.api.dag

import collection.mutable
import mutable.{ArrayBuffer}
import collection.distributed.api.{ReifiedDistCollection, UniqueId}

case class OutputPlanNode(collection: ReifiedDistCollection,
                          inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer,
                          outEdges: mutable.LinkedHashMap[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.LinkedHashMap,
                          uniqueId: Long = UniqueId()) extends IOPlanNode {
  def copyUnconnected() = copy(inEdges = new ArrayBuffer, outEdges = new mutable.LinkedHashMap)

  override def nodeType = "O"
}