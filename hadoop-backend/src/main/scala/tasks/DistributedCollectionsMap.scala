package tasks

import dag.{InputRuntimePlanNode, RuntimePlanNode, RuntimeDAG}
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

class DistributedCollectionsMap extends MapReduceBase
with Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] with CollectionTask {

  var distContext: DistContext = new DistContext
  var mapDAG: ExPlanDAG = null
  var mapRuntimeDAG: RuntimeDAG = null
  var intermediateOutputs: Traversable[URI] = null
  var multipleOutputs: MultipleOutputs = null
  var recordsInitialized = false
  var myInput: InputRuntimePlanNode = null
  var tempFileToURI: mutable.Map[String, URI] = null
  var workingDir: Path = null


  override def configure(job: JobConf) = {
    super.configure(job)

    mapDAG = deserializeFromCache(job, "distribted-collections.mapDAG").get
    intermediateOutputs = deserializeFromCache(job, "distribted-collections.intermediateOutputs").get
    tempFileToURI = deserializeFromCache(job, "distribted-collections.tempFileToURI").get
    multipleOutputs = new MultipleOutputs(job)

    workingDir = job.getWorkingDirectory
  }

  override def close = multipleOutputs.close


  def map(key: NullWritable, value: BytesWritable, output: OutputCollector[BytesWritable, BytesWritable], reporter: Reporter) = {
    if (!recordsInitialized) {
      val fileSplit = reporter.getInputSplit.asInstanceOf[FileSplit]
      val fileNameParts = fileSplit.getPath().getName.split("-")

      val fileNumber = Integer.parseInt(fileNameParts(fileNameParts.length - 1))
      val recordStart = fileSplit.getStart
      distContext.recordNumber = new RecordNumber(fileNumber, recordStart, 0L)

      mapRuntimeDAG = RuntimeDAG(mapDAG, multipleOutputs, output.asInstanceOf[OutputCollector[Writable, Writable]],
        tempFileToURI.map(v => (v._2, v._1)), intermediateOutputs.toSet, reporter)
      mapRuntimeDAG.initialize
      myInput = mapRuntimeDAG.inputNodes.find(v => new Path(workingDir, v.node.collection.location.toString).toString == fileSplit.getPath.getParent.toString).get

      recordsInitialized = true
    }

    myInput.execute(null, distContext, null, value.getBytes)
    distContext.recordNumber.incrementRecordCounter
  }

}
