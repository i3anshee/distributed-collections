package scala.collection.distributed.api.dag

import collection.mutable
import collection.distributed.api.{ReifiedDistCollection, UniqueId}
import mutable.ArrayBuffer

trait PlanNode extends UniqueId {

  val inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)]
  val outEdges: mutable.LinkedHashMap[ReifiedDistCollection, mutable.Buffer[PlanNode]]

  protected[this] val uniqueId: Long

  def copyUnconnected: PlanNode

  def disconnect(that: PlanNode) {
    val exists = (children.exists(_ == that) || inEdges.contains(that))
    outEdges.values.foreach(_ filterNot (_ == that))
    val remaining = inEdges.filter(_._1 == that)
    inEdges.clear()
    inEdges ++= remaining

    if (exists)
      that.disconnect(this)
  }

  def connect(toOutput: ReifiedDistCollection, node: PlanNode) {
    outEdges.put(toOutput, outEdges.getOrElse(toOutput, new ArrayBuffer[PlanNode]) += node)
    node.inEdges += ((this, toOutput))
  }

  def children: Iterable[PlanNode] = outEdges.values.flatMap(v => v)

  def predecessors: Iterable[PlanNode] = inEdges.map(_._1)

  def disconnect() {
    (predecessors ++ children).foreach(_.disconnect(this))
  }

  override def toString = nodeType + "(" + id + ")->" +
    children.map(node => "" + node.nodeType + "(" + node.id + ")").mkString("[", ",", "]")

  def nodeType: String

}