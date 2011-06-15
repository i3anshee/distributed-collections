package tasks.dag

import collection.distributed.api.dag.{InputPlanNode, IOPlanNode}
import java.io.{ObjectInputStream, ByteArrayInputStream}
import collection.distributed.api.DistContext
import collection.JavaConversions._
import org.apache.hadoop.io.BytesWritable
import collection.mutable.ArrayBuffer

/**
 * @author Vojin Jovanovic
 */

abstract class InputRuntimePlanNode(val node: InputPlanNode) extends IOPlanNode with RuntimePlanNode {
  val collection = node.collection

  def deserializeElement(bytes: Array[Byte]): AnyRef = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject()
  }

}

class MapInputRuntimePlanNode(node: InputPlanNode) extends InputRuntimePlanNode(node) {
  def copyUnconnected() = new MapInputRuntimePlanNode(node)

  override def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) =
    emitter.emit(deserializeElement(value.asInstanceOf[Array[Byte]]))
}

class ReduceInputRuntimePlanNode(node: InputPlanNode) extends InputRuntimePlanNode(node) {
  def copyUnconnected() = new ReduceInputRuntimePlanNode(node)

  override def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) = {
    val deserializedValue = new ArrayBuffer[Any]
    asScalaIterator(value.asInstanceOf[java.util.Iterator[BytesWritable]]).foreach(v => deserializedValue += deserializeElement(v.getBytes).asInstanceOf[Any])
    emitter.emit((key, deserializedValue.toList))
  }

}

