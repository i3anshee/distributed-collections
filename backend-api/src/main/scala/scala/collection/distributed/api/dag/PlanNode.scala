package scala.collection.distributed.api.dag

import collection.distributed.api.{UniqueId, CollectionId}
import collection.mutable

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

abstract case class PlanNode extends UniqueId {
  type EdgeData = CollectionId

  var inEdges: mutable.Map[PlanNode, EdgeData] = new mutable.HashMap()
  var outEdges: mutable.Map[PlanNode, EdgeData] = new mutable.HashMap()

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
}
