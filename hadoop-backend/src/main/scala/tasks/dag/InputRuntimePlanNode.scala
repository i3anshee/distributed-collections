package tasks.dag

import collection.distributed.api.dag.{InputPlanNode, IOPlanNode}
import collection.distributed.api.DistContext
import collection.JavaConversions._
import org.apache.hadoop.io.BytesWritable
import collection.mutable.ArrayBuffer
import collection.distributed.api.io.SerializerInstance

/**
 * @author Vojin Jovanovic
 */

abstract class InputRuntimePlanNode(val node: InputPlanNode, val serializerInstance: SerializerInstance) extends IOPlanNode with RuntimePlanNode {
  val collection = node.collection
}

class MapInputRuntimePlanNode(node: InputPlanNode, serializerInstance: SerializerInstance)
  extends InputRuntimePlanNode(node, serializerInstance) {
  def copyUnconnected() = new MapInputRuntimePlanNode(node, serializerInstance)

  override def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) =
    emit(serializerInstance.deserialize(value.asInstanceOf[Array[Byte]]))
}

class ReduceInputRuntimePlanNode(node: InputPlanNode, serializerInstance: SerializerInstance)
  extends InputRuntimePlanNode(node, serializerInstance) {

  def copyUnconnected() = new ReduceInputRuntimePlanNode(node, serializerInstance)

  override def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) = {
    val deserializedValue = new ArrayBuffer[Any]
    asScalaIterator(value.asInstanceOf[java.util.Iterator[BytesWritable]]).foreach(v => deserializedValue += serializerInstance.deserialize(v.getBytes).asInstanceOf[Any])
    emit((key, deserializedValue.toList))
  }
}