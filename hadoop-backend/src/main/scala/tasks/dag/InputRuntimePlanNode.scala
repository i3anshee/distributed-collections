package tasks.dag

import collection.distributed.api.dag.{InputPlanNode, IOPlanNode}
import java.io.{ObjectInputStream, ByteArrayInputStream}
import collection.distributed.api.DistContext

/**
 * @author Vojin Jovanovic
 */

class InputRuntimePlanNode(val node: InputPlanNode) extends IOPlanNode with RuntimePlanNode {
  def copyUnconnected() = new InputRuntimePlanNode(node)

  val collection = node.collection

  def deserializeElement(bytes: Array[Byte]): AnyRef = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject()
  }

  override def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) = emitter.emit(deserializeElement(value.asInstanceOf[Array[Byte]]))
}