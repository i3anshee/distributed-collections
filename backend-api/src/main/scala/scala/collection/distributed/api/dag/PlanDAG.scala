package scala.collection.distributed.api.dag

import collection.distributed.api.{ReifiedDistCollection, UniqueId}
import collection.mutable

/**
 * @author Vojin Jovanovic
 */

class PlanDAG[T <: PlanNode, I <: IOPlanNode](val inputNodes: mutable.Set[I] = new mutable.HashSet[I]) extends Traversable[T]
with Serializable {

  def addInputNode(inputPlanNode: I) = inputNodes.add(inputPlanNode)

  def getPlanNode(uid: UniqueId): Option[T] = find(_.id == uid.id)

  def getPlanNode(id: ReifiedDistCollection): Option[T] = this.find(_.outEdges.contains(id))

  def foreach[U](f: (T) => U) = {
    val queue: mutable.Queue[PlanNode] = (new mutable.Queue ++= inputNodes)
    val visited = new mutable.HashSet[PlanNode]
    while (!queue.isEmpty) {
      val node = queue.dequeue
      if (!visited.contains(node)) {
        visited += node
        f(node.asInstanceOf[T])
      }
      queue ++= node.children
    }
  }

  def connectDownwards(newNode: PlanNode, oldNode: PlanNode) =
    oldNode.outEdges.foreach(outputs => {
      outputs._2.foreach(node => {
        val child = getPlanNode(node)
        if (child.isDefined)
          newNode.connect(outputs._1, child.get)
      })
    })

  def connectUpwards(newNode: PlanNode, oldNode: PlanNode) {
    oldNode.inEdges.foreach(node => {
      val parent = getPlanNode(node._1)
      if (parent.isDefined)
        parent.get.connect(node._2, newNode)
    })
  }

  def connect(newNode: PlanNode, oldNode: PlanNode) {
    connectUpwards(newNode, oldNode)
    connectDownwards(newNode, oldNode)
  }

  override def toString = {
    // not using Traversable because the traversal order is special
    var newQueue = new mutable.Queue[PlanNode]() ++= inputNodes

    val visited = new mutable.HashSet[PlanNode]
    val sb = new StringBuilder("BFS = [\n")
    while (!newQueue.isEmpty) {
      val queue = newQueue
      newQueue = new mutable.Queue[PlanNode]
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