package scala.collection.distributed.api.dag

import scala.collection.immutable
import scala.collection.mutable
import collection.distributed.api.{ReifiedDistCollection, UniqueId}

class ExPlanDAG(val inputNodes: mutable.Set[InputPlanNode] = mutable.Set[InputPlanNode]()) extends Traversable[PlanNode]
with Serializable {

  def addInputNode(inputPlanNode: InputPlanNode) = inputNodes.add(inputPlanNode)

  def getPlanNode(uid: UniqueId): Option[PlanNode] = find(_.id == uid.id)

  def getPlanNode(id: ReifiedDistCollection): Option[PlanNode] = this.find(_.outEdges.contains(id))

  def foreach[U](f: (PlanNode) => U) = {
    val queue = (new mutable.Queue[PlanNode]() ++= inputNodes)
    val visited = new mutable.HashSet[PlanNode]
    while (!queue.isEmpty) {
      val node = queue.dequeue
      if (!visited.contains(node)) {
        visited += node
        f(node)
      }
      queue ++= node.children
    }
  }

  override def toString = {
    // not using Traversable because the traversal order is special
    var newQueue = new mutable.Queue[PlanNode]() ++= inputNodes

    val visited = new mutable.HashSet[PlanNode]()
    val sb = new StringBuilder("BFS = [\n")
    while (!newQueue.isEmpty) {
      val queue = newQueue
      newQueue = new mutable.Queue[PlanNode]()
      sb.append("\n")
      while (!queue.isEmpty) {
        val node = queue.dequeue
        if (!visited(node)) {
          sb.append(node.toString()).append(", ")
          newQueue ++= node.children
          visited.add(node)
        }

      }
      sb.append("\n")
    }
    sb.append("]")
    sb.toString
  }

}