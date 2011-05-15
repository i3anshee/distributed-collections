package tasks.dag

import collection.distributed.api.dag.{IOPlanNode, OutputPlanNode}
import collection.distributed.api.DistContext
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.io.{NullWritable, BytesWritable, Writable}
import java.io.{ObjectOutputStream, ByteArrayOutputStream}

/**
 * @author Vojin Jovanovic
 */

class OutputRuntimePlanNode(val node: OutputPlanNode, val collector: OutputCollector[Writable, Writable]) extends IOPlanNode with RuntimePlanNode {
  def copyUnconnected() = new OutputRuntimePlanNode(node, collector)

  val collection = node.collection


  override def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) =
    collector.collect(NullWritable.get, new BytesWritable(serializeElement(value)))


  def serializeElement(value: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.flush()
    baos.toByteArray
  }

}

class MapOutputRuntimePlanNode(node: OutputPlanNode, collector: OutputCollector[Writable, Writable], val byteId: Byte)
  extends OutputRuntimePlanNode(node, collector) {
  override def copyUnconnected() = new MapOutputRuntimePlanNode(node, collector, byteId)

  override def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) =
    collector.collect(new BytesWritable(serializeElement((byteId, key))), new BytesWritable(serializeElement(value)))
}