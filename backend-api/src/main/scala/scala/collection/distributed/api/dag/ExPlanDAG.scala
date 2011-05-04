package scala.collection.distributed.api.dag

import scala.collection.immutable
import scala.collection.mutable
import collection.distributed.api.{UniqueId, CollectionId}

class ExPlanDAG(val inputNodes: immutable.Set[InputPlanNode] = Set[InputPlanNode]()) {

  def addInputNode(inputPlanNode: InputPlanNode): ExPlanDAG = {
    new ExPlanDAG(inputNodes + inputPlanNode)
  }

  def getPlanNode(id: UniqueId): Option[PlanNode] = {
    val queue = new scala.collection.mutable.Queue[PlanNode]() ++= inputNodes

    var res: Option[PlanNode] = None
    while (res.isEmpty && !queue.isEmpty) {
      val node = queue.dequeue
      if (node.id == id) res = Some(node)

      queue ++= node.outEdges.keys
    }
    res
  }

  def getPlanNode(id: CollectionId): Option[PlanNode] = {
    val queue = new mutable.Queue[PlanNode]() ++= inputNodes

    var res: Option[PlanNode] = None
    while (res.isEmpty && !queue.isEmpty) {
      val node = queue.dequeue
      node match {
        case v: PlanNode with CollectionId =>
          if (v.location == id.location) res = Some(node)
        case _ => None
      }

      queue ++= node.outEdges.keys
    }
    res
  }

}