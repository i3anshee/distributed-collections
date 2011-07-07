package tasks

import org.apache.hadoop.io.BytesWritable
import java.util.Iterator
import org.apache.hadoop.mapred._
import java.net.URI
import collection.distributed.api.dag.CombinePlanNode
import collection.mutable
import collection.JavaConversions._
import collection.distributed.api.io.{JavaSerializerInstance, SerializerInstance}
import io.KryoSerializer

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class DistributedCollectionsCombine extends MapReduceBase with Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable] with CollectionTask {
  val byteToNode: mutable.Map[Byte, CombinePlanNode[Any, Any]] = new mutable.HashMap

  override def configure(job: JobConf) = {
    val ser = job.get("serializatorRegistrator")
    if (ser != null) {
      System.setProperty("spark.kryo.registrator", ser);
    }
    serializerInstance = new KryoSerializer().newInstance()

    val intermediateToByte: mutable.Map[URI, Byte] = deserializeFromCache(job, "distribted-collections.intermediateToByte").get
    val combinedInputs: mutable.Map[URI, CombinePlanNode[Any, Any]] = deserializeFromCache(job, "distribted-collections.combinedInputs").get
    byteToNode ++= intermediateToByte.map(v => (v._2, combinedInputs(v._1)))
  }

  def reduce(key: BytesWritable, values: Iterator[BytesWritable], output: OutputCollector[BytesWritable, BytesWritable], reporter: Reporter) = {
    val collKeyPair: (Byte, Any) = serializerInstance.deserialize(key.getBytes)

    val nodeOp = byteToNode.get(collKeyPair._1)

    // TODO (VJ) inspect this line of code. Is it necessary, what is happening with performacne.
    if (nodeOp.isDefined) {
      val data = serializerInstance.serialize(
        nodeOp.get.op(values.toIterable.map(v => serializerInstance.deserialize(v.getBytes).asInstanceOf[Any])))


      output.collect(key, new BytesWritable(data))
    } else {
      values.foreach(v => output.collect(key, v))
    }

  }
}