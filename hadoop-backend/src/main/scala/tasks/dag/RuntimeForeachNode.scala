package tasks.dag

import collection.distributed.api.dag.PlanNode
import collection.distributed.api.shared.{CollectionType, DistSideEffects}
import collection.distributed.hadoop.shared.DistBuilderNode
import collection.mutable.Buffer
import collection.distributed.api.ReifiedDistCollection

/**
 * @author Vojin Jovanovic
 */

class RuntimeForeachNode(val node: PlanNode) extends RuntimePlanNode {
  def copyUnconnected = new RuntimeForeachNode(node)

  override def initialize() {
    super.initialize()
    // find all the builders
    DistSideEffects.sideEffectsData.keys.foreach(v => {
      if (v.varType == CollectionType) {
        val builder = v.impl.asInstanceOf[DistBuilderNode]
        if (outEdges.contains(ReifiedDistCollection(builder))) {
          builder.output = (this, outEdges.get(builder).get.asInstanceOf[Buffer[tasks.dag.RuntimePlanNode]])
        }
      }
    })
  }

}