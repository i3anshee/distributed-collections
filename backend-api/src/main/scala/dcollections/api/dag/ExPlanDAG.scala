package dcollections.api.dag

import dcollections.api.CollectionId

/**
 * User: vjovanovic
 * Date: 4/1/11
 */

class ExPlanDAG {
  var inputNodes = Set[InputPlanNode]()
  var outputNodes = Set[OutputPlanNode]()

  def addInputNode(inputPlanNode: InputPlanNode) = {
    inputNodes += inputPlanNode
  }

  def getPlanNode(id: CollectionId): Option[PlanNode] = {
    val queue = new scala.collection.mutable.Queue[PlanNode]() ++= inputNodes

    var res: Option[PlanNode] = None
    while (res.isEmpty && !queue.isEmpty) {
      val node = queue.dequeue
      if (node.id == id) res = Some(node)

      queue ++= node.outEdges
    }
    res
  }
}