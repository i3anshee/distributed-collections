package tasks.dag

import org.apache.hadoop.mapred.lib.MultipleOutputs
import collection.distributed.api.dag._
import collection.mutable
import org.apache.hadoop.mapred.{Reporter, OutputCollector}
import java.net.URI
import org.apache.hadoop.io.Writable

/**
 * @author Vojin Jovanovic
 */

class RuntimeDAG extends PlanDAG[RuntimePlanNode, InputRuntimePlanNode] {
  def initialize() = foreach(_.initialize)

}

object RuntimeDAG {
  def apply(plan: ExPlanDAG, outputs: MultipleOutputs, collector: OutputCollector[Writable, Writable], tempFileToURI: mutable.Map[URI, String],
            intermediateSet: Set[URI], reporter: Reporter): RuntimeDAG = {
    val runtimeDAG = new RuntimeDAG
    plan.foreach(node => node match {
      case v: InputPlanNode =>
        val copiedNode = new InputRuntimePlanNode(v)
        runtimeDAG.addInputNode(copiedNode)
        runtimeDAG.connect(copiedNode, v)

      case v: OutputPlanNode =>
      val tempFile =
        if (intermediateSet.contains(v.collection.location))
          runtimeDAG.connect(new OutputRuntimePlanNode(v, collector), v)
        else
          runtimeDAG.connect(new OutputRuntimePlanNode(v, outputs.getCollector(tempFileToURI(v.collection.location), reporter).asInstanceOf[OutputCollector[Writable, Writable]]), v)

      case _ => // copy the node to runtimeDAG with all connections
        runtimeDAG.connect(new RuntimeComputationNode(node), node)
    })
    runtimeDAG
  }
}