package tasks

import dag._
import org.apache.hadoop.mapred._
import lib.MultipleOutputs
import java.net.URI
import collection.distributed.api.{RecordNumber, DistContext}
import collection.distributed.api.dag.{IOPlanNode, OutputPlanNode, InputPlanNode, ExPlanDAG}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Writable, NullWritable, BytesWritable}
import collection.mutable

/**
 * User: vjovanovic
 * Date: 4/4/11
 */

class DistributedCollectionsMapRunner extends MapRunnable[NullWritable, BytesWritable, BytesWritable, BytesWritable] with CollectionTask {

  var distContext: DistContext = new DistContext
  var mapDAG: ExPlanDAG = null
  var mapRuntimeDAG: RuntimeDAG = null
  var intermediateOutputs: Traversable[URI] = null
  var multipleOutputs: MultipleOutputs = null
  var myInput: InputRuntimePlanNode = null
  var tempFileToURI: mutable.Map[String, URI] = null
  var intermediateToByte: mutable.Map[URI, Byte] = null

  var workingDir: Path = null

  var incrProcCount: Boolean = false

  def configure(job: JobConf) = {

    mapDAG = deserializeFromCache(job, "distribted-collections.mapDAG").get
    intermediateOutputs = deserializeFromCache(job, "distribted-collections.intermediateOutputs").get
    tempFileToURI = deserializeFromCache(job, "distribted-collections.tempFileToURI").get
    intermediateToByte = deserializeFromCache(job, "distribted-collections.intermediateToByte").get
    multipleOutputs = new MultipleOutputs(job)

    workingDir = job.getWorkingDirectory

    this.incrProcCount = SkipBadRecords.getMapperMaxSkipRecords(job) > 0 && SkipBadRecords.getAutoIncrMapperProcCount(job)
  }


  def run(input: RecordReader[NullWritable, BytesWritable], output: OutputCollector[BytesWritable, BytesWritable], reporter: Reporter): Unit = {
    try {
      val fileSplit = reporter.getInputSplit.asInstanceOf[FileSplit]
      val fileNameParts = fileSplit.getPath().getName.split("-")

      val fileNumber = Integer.parseInt(fileNameParts(fileNameParts.length - 1))
      val recordStart = fileSplit.getStart
      distContext.recordNumber = new RecordNumber(fileNumber, recordStart, 0L)

      mapRuntimeDAG = buildRuntimeDAG(mapDAG, multipleOutputs, output.asInstanceOf[OutputCollector[Writable, Writable]],
        tempFileToURI.map(v => (v._2, v._1)), intermediateOutputs.toSet, reporter)
      mapRuntimeDAG.initialize
      myInput = mapRuntimeDAG.inputNodes.find(v => new Path(workingDir, v.node.collection.location.toString).toString == fileSplit.getPath.getParent.toString).get

      var key: NullWritable = input.createKey
      var value: BytesWritable = input.createValue

      while (input.next(key, value)) {
        myInput.execute(null, distContext, null, value.getBytes)
        distContext.recordNumber.incrementRecordCounter

        if (incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1)
        }
      }
    }
    finally {
      multipleOutputs.close
    }
  }

  def buildRuntimeDAG(plan: ExPlanDAG, outputs: MultipleOutputs, collector: OutputCollector[Writable, Writable], tempFileToURI: mutable.Map[URI, String],
                      intermediateSet: Set[URI], reporter: Reporter): RuntimeDAG = {
    val runtimeDAG = new RuntimeDAG
    plan.foreach(node => node match {
      case v: InputPlanNode =>
        val copiedNode = new MapInputRuntimePlanNode(v)
        runtimeDAG.addInputNode(copiedNode)
        runtimeDAG.connect(copiedNode, v)

      case v: OutputPlanNode =>
      val tempFile =
        if (intermediateSet.contains(v.collection.location))
          runtimeDAG.connect(new MapOutputRuntimePlanNode(v, collector, intermediateToByte(v.collection.location)), v)
        else
          runtimeDAG.connect(new OutputRuntimePlanNode(v, outputs.getCollector(tempFileToURI(v.collection.location), reporter).asInstanceOf[OutputCollector[Writable, Writable]]), v)

      case _ => // copy the node to runtimeDAG with all connections
        runtimeDAG.connect(new RuntimeComputationNode(node), node)
    })
    runtimeDAG
  }


}
