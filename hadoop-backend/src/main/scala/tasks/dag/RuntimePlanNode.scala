package tasks.dag

import collection.mutable
import collection.distributed.api.dag._
import mutable.{Buffer, ArrayBuffer}
import collection.distributed.api.{ReifiedDistCollection, DistContext}

/**
 * @author Vojin Jovanovic
 */

trait RuntimePlanNode extends PlanNode {
  val node: PlanNode

  def nodeType = node.nodeType

  protected[this] val uniqueId = node.id

  protected[this] var outputs = Buffer[(scala.collection.distributed.api.DistContext, scala.collection.mutable.Buffer[tasks.dag.RuntimePlanNode])]()

  val inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer

  val outEdges: mutable.LinkedHashMap[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.LinkedHashMap

  def initialize: Unit = outputs ++= node.outEdges.toSeq.map(v => (new DistContext(), v._2.asInstanceOf[Buffer[RuntimePlanNode]]))

  def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any): Unit = this.node match {
    case v: FlattenPlanNode => emit(value)

    case v: DistForeachPlanNode[Any, Any] => v.distOP(value)

    case v: CombinePlanNode[Any, Any] =>
      val (k, it) = value.asInstanceOf[Tuple2[Any, Iterable[Any]]]
      emit((k, v.op(it)))

    case v: GroupByPlanNode[Any, Any, Any] =>
      outputs.foreach {
        output =>
        // TODO (VJ) what happens if we have multiple nodes here
          output._2.foreach(v => v.execute(this, output._1, key, value))
          output._1.recordNumber.incrementRecordCounter
      }
  }

  protected[this] def emit(el: Any) = {
    outputs.foreach {
      output =>
        //TODO (VJ) remove this null somehow ( maybe just one input and in case of group by plan node we know that it is a tuple)
        output._2.foreach(v => v.execute(this, output._1, null, el))
        output._1.recordNumber.incrementRecordCounter
    }
  }
}