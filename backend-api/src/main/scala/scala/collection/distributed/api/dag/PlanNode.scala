package scala.collection.distributed.api.dag

import collection.distributed.api.{UniqueId, CollectionId}
import collection.mutable
import java.net.URI

abstract class PlanNode extends UniqueId  {
  type EdgeData = CollectionId

  val inEdges: mutable.Map[PlanNode, EdgeData]
  val outEdges: mutable.Map[PlanNode, EdgeData]
  protected[this] val uniqueId: Long

  def disconnect(that: PlanNode) {
    that.disconnect(this)
    inEdges.remove(that)
    outEdges.remove(that)
  }

  def connect(inNode: PlanNode, ed: EdgeData) {
    outEdges.put(inNode, ed)
    inNode.inEdges.put(this, ed)
  }

  def disconnect() {
    outEdges.keys.foreach(v => v.disconnect(this))
    inEdges.keys.foreach(v => v.disconnect(this))
  }

  def copyUnconnected(): PlanNode
}

object PlanNode {
  def main(args: Array[String]) {
    val node = InputPlanNode(new URI("www.google.com"))

    val copy = node.copy(new URI("www.yahoo.com"))

    println("nodeId=" + node.id + "copyId=" + copy.id)
  }
}