package tasks.dag

import collection.distributed.api.dag.{IOPlanNode, OutputPlanNode}
import collection.distributed.api.DistContext
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.io.{NullWritable, BytesWritable, Writable}
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import collection.distributed.api.io.SerializerInstance

/**
 * @author Vojin Jovanovic
 */

class OutputRuntimePlanNode(val node: OutputPlanNode,
                            val serializerInstace: SerializerInstance,
                            val collector: OutputCollector[Writable, Writable])
  extends IOPlanNode with RuntimePlanNode {
  def copyUnconnected() = new OutputRuntimePlanNode(node, serializerInstace, collector)

  val collection = node.collection


  override def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) = {
    collector.collect(NullWritable.get, new BytesWritable(serializerInstace.serialize(value)))
  }
}

class MapOutputRuntimePlanNode(node: OutputPlanNode,
                               collector: OutputCollector[Writable, Writable],
                               serializerInstance: SerializerInstance,
                               val byteId: Byte)
  extends OutputRuntimePlanNode(node, serializerInstance, collector) {

  override def copyUnconnected() = new MapOutputRuntimePlanNode(node, collector, serializerInstance, byteId)

  override def execute(parent: RuntimePlanNode, context: DistContext, key: Any, value: Any) = {
    val data = serializerInstace.serialize(value)
    val keyData = serializerInstance.serialize((byteId, key))
    collector.collect(new BytesWritable(keyData), new BytesWritable(data))

  }
}