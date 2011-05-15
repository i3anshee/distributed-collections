package tasks

import org.apache.hadoop.io.BytesWritable
import java.util.Iterator
import org.apache.hadoop.mapred._
import java.net.URI
import collection.distributed.api.dag.CombinePlanNode
import collection.mutable
import collection.JavaConversions._
import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class DistributedCollectionsCombine extends MapReduceBase with Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable] with CollectionTask {
  val byteToNode: mutable.Map[Byte, CombinePlanNode[Any, Any]] = new mutable.HashMap

  override def configure(job: JobConf) = {
    val intermediateToByte: mutable.Map[URI, Byte] = deserializeFromCache(job, "distribted-collections.intermediateToByte").get
    val combinedInputs: mutable.Map[URI, CombinePlanNode[Any, Any]] = deserializeFromCache(job, "distribted-collections.combinedInputs").get
    byteToNode ++= intermediateToByte.map(v => (v._2, combinedInputs(v._1)))
  }

  def reduce(key: BytesWritable, values: Iterator[BytesWritable], output: OutputCollector[BytesWritable, BytesWritable], reporter: Reporter) = {
    val collKeyPair = deserializeElement(key.getBytes).asInstanceOf[(Byte, Any)]

    val nodeOp = byteToNode.get(collKeyPair._1)
    if (nodeOp.isDefined)
      output.collect(key, new BytesWritable(
        serializeElement(nodeOp.get.op(values.toIterable.map(v => deserializeElement(v.getBytes).asInstanceOf[Any])))))
    else
      values.foreach(v => output.collect(key, v))
  }

   def serializeElement(value: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.flush()
    baos.toByteArray
  }

  def deserializeElement(bytes: Array[Byte]): AnyRef = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject()
  }
}