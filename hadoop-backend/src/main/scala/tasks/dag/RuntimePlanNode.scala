package tasks.dag

import collection.mutable
import collection.distributed.api.dag._
import mutable.{Buffer, ArrayBuffer}
import tasks.BufferEmitter
import collection.distributed.api.{ReifiedDistCollection, DistContext, UntypedEmitter}

/**
 * @author Vojin Jovanovic
 */

trait RuntimePlanNode extends PlanNode {
  val node: PlanNode

  protected[this] var myEmitter: UntypedEmitter = null

  def emitter: UntypedEmitter = myEmitter

  def nodeType = node.nodeType

  protected[this] val uniqueId = node.id

  val inEdges: mutable.Buffer[(PlanNode, ReifiedDistCollection)] = new ArrayBuffer

  val outEdges: mutable.LinkedHashMap[ReifiedDistCollection, mutable.Buffer[PlanNode]] = new mutable.LinkedHashMap


  def initialize = node match {
    case v: GroupByPlanNode[_, _, _] =>
      myEmitter = new GroupByRuntimeEmitter(this, outEdges.toSeq.map(v => (new DistContext(), v._2.asInstanceOf[Buffer[RuntimePlanNode]])))

    case _ =>
      myEmitter = new RuntimeUntypedEmitter(this, outEdges.toSeq.map(v => (new DistContext(), v._2.asInstanceOf[Buffer[RuntimePlanNode]])))
  }

  def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) = this.node match {
    case v: FlattenPlanNode => emitter.emit(value)

    case v: DistDoPlanNode[Any] => v.distOP(value, emitter, context)

    case v: CombinePlanNode[Any, Any] =>
      val (k, it) = value.asInstanceOf[Tuple2[Any, Iterable[Any]]]
      emitter.emit((k, v.op(it)))

    case v: GroupByPlanNode[Any, Any, Any] =>
      val tmpEmitter = new BufferEmitter(1)
      val key = v.keyFunction(value, tmpEmitter)

      tmpEmitter.getBuffer(0).foreach(v => emitter.emit((key, v)))
  }

}

class RuntimeUntypedEmitter(val node: RuntimePlanNode, val outputs: Seq[(DistContext, mutable.Buffer[RuntimePlanNode])]) extends UntypedEmitter {

  def emit(index: Int, el: Any) = {
    val output = outputs(index)
    output._2.foreach(v => v.execute(node, output._1, null, el))
    outputs(index)._1.recordNumber.incrementRecordCounter
  }

}

class GroupByRuntimeEmitter(val node: RuntimePlanNode, val outputs: Seq[(DistContext, mutable.Buffer[RuntimePlanNode])]) extends UntypedEmitter {

  def emit(index: Int, el: Any) = {
    val typedEl = el.asInstanceOf[(Any, Any)]

    // TODO (VJ) group by result crosses the barrier once and then it is separated (optimization)
    val output = outputs(index)
    output._2.foreach(v => v.execute(node, output._1, typedEl._1, typedEl._2))
    outputs(index)._1.recordNumber.incrementRecordCounter
  }

}
