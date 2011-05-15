package tasks

import dag._
import java.util.Iterator
import org.apache.hadoop.mapred._
import lib.MultipleOutputs
import org.apache.hadoop.io.{Writable, BytesWritable, NullWritable}
import collection.distributed.api.dag.{InputPlanNode, OutputPlanNode, ExPlanDAG}
import java.net.URI
import collection.mutable
import org.apache.hadoop.fs.Path
import collection.distributed.api.{ReifiedDistCollection, DistContext}
import java.io.{ByteArrayInputStream, ObjectInputStream}

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class DistributedCollectionsReduce extends MapReduceBase with Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] with CollectionTask {

  var distContext: DistContext = null
  var reduceDAG: ExPlanDAG = null

  var reduceRuntimeDAG: RuntimeDAG = null
  var intermediateOutputs: Traversable[URI] = null
  var workingDir: Path = null
  var tempFileToURI: mutable.Map[String, URI] = null
  var intermediateToByte: mutable.Map[URI, Byte] = new mutable.HashMap
  val byteToNode: mutable.Map[Byte, RuntimePlanNode] = new mutable.HashMap

  var multipleOutputs: MultipleOutputs = null
  var initialized = false

  override def configure(job: JobConf) = {
    super.configure(job)

    reduceDAG = deserializeFromCache(job, "distribted-collections.reduceDAG").get
    tempFileToURI = deserializeFromCache(job, "distribted-collections.tempFileToURI").get
    intermediateToByte = deserializeFromCache(job, "distribted-collections.intermediateToByte").get


    multipleOutputs = new MultipleOutputs(job)

    distContext = new DistContext

  }

  override def close = multipleOutputs.close

  def reduce(key: BytesWritable, values: Iterator[BytesWritable], doNotUseThisOutput: OutputCollector[NullWritable, BytesWritable], reporter: Reporter) = {
    if (!initialized) {
      // create a dag
      reduceRuntimeDAG = buildRuntimeDAG(reduceDAG, multipleOutputs, tempFileToURI.map(v => (v._2, v._1)), reporter)
      reduceRuntimeDAG.initialize

      byteToNode ++= intermediateToByte.map(v => (v._2, reduceRuntimeDAG.getPlanNode((ReifiedDistCollection(v._1, manifest[Any]))).get))

      initialized = true
    }

    val collKeyPair = deserializeElement[(Byte, Any)](key.getBytes)
    byteToNode(collKeyPair._1).execute(null, distContext, collKeyPair._2, values)
  }

  def deserializeElement[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject().asInstanceOf[T]
  }

  def buildRuntimeDAG(plan: ExPlanDAG, outputs: MultipleOutputs, tempFileToURI: mutable.Map[URI, String], reporter: Reporter): RuntimeDAG = {
    val runtimeDAG = new RuntimeDAG
    plan.foreach(node => node match {
      case v: InputPlanNode =>
        val copiedNode = new ReduceInputRuntimePlanNode(v)
        runtimeDAG.addInputNode(copiedNode)
        runtimeDAG.connect(copiedNode, v)

      case v: OutputPlanNode =>
        runtimeDAG.connect(new OutputRuntimePlanNode(v, outputs.getCollector(tempFileToURI(v.collection.location), reporter).asInstanceOf[OutputCollector[Writable, Writable]]), v)

      case _ => // copy the node to runtimeDAG with all connections
        runtimeDAG.connect(new RuntimeComputationNode(node), node)
    })
    runtimeDAG
  }
}
