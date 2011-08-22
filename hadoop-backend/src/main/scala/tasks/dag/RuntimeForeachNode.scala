package tasks.dag

import collection.distributed.api.dag.PlanNode
import collection.distributed.api.shared.{CollectionType, DistSideEffects}
import colleciton.distributed.hadoop.shared.DistBuilderNode
import collection.mutable.Buffer

/**
 * @author Vojin Jovanovic
 */

class RuntimeForeachNode(val node: PlanNode) extends RuntimePlanNode {
  def copyUnconnected() = new RuntimeForeachNode(node)

  override def initialize = {
    // find all the builders
    DistSideEffects.sideEffectsData.keys.foreach(v => {
      if (v.varType == CollectionType) {
        val builder = v.impl.asInstanceOf[DistBuilderNode]
        if (outEdges.contains(builder)) {
          builder.output = (this, outEdges.get(builder).get.asInstanceOf[Buffer[tasks.dag.RuntimePlanNode]])
        }
      }
    })
  }

}