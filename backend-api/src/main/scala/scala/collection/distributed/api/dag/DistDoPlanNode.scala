package scala.collection.distributed.api.dag

import collection.immutable.GenSeq
import collection.mutable
import collection.distributed.api._
import mutable.{Buffer, ArrayBuffer}

case class DistDoPlanNode[T](distOP: (T, UntypedEmitter, DistContext) => Unit,
                             inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer,
                             outEdges: mutable.Map[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.HashMap,
                             uniqueId: Long = UniqueId()) extends PlanNode {
  def copyUnconnected() = copy(inEdges = new ArrayBuffer, outEdges = new mutable.HashMap)

  override def nodeType = "DO"
}
