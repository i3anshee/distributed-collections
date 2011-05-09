package tasks

import scala.collection.distributed.api.DistContext
import org.apache.hadoop.mapred._
import collection.distributed.api.dag.ExPlanDAG
import org.apache.hadoop.io.{NullWritable, BytesWritable}

/**
 * User: vjovanovic
 * Date: 4/4/11
 */

class DistributedCollectionsMap extends MapReduceBase
with Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] with CollectionTask {

  var distContext: DistContext = null
  var mapDAG: ExPlanDAG = null

  override def configure(job: JobConf) = {
    super.configure(job)

    mapDAG = deserializeFromCache(job, "distribted-collections.mapDAG").get
  }

  override def close = {}


  def map(key: NullWritable, value: BytesWritable, output: OutputCollector[BytesWritable, BytesWritable], reporter: Reporter) = {

  }

  //  override def run(context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]) = {
  //    setup(context)
  //
  //    val fileSplit = context.getInputSplit.asInstanceOf[FileSplit]
  //    val fileNameParts = fileSplit.getPath().getName.split("-")
  //
  //    val fileNumber = Integer.parseInt(fileNameParts(fileNameParts.length - 1))
  //    val recordStart = fileSplit.getStart
  //    distContext.recordNumber = new RecordNumber(fileNumber, recordStart, 0L)
  //
  //    while (context.nextKeyValue()) {
  //      distMap(context.getCurrentKey(), context.getCurrentValue(), context, distContext)
  //      distContext.recordNumber.incrementRecordCounter()
  //    }
  //
  //    cleanup(context)
  //  }
  //
  //  def distMap(key: NullWritable, value: BytesWritable, context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable], distContext: DistContext)
  //
  //  protected def randomKey(): BytesWritable = {
  //    new BytesWritable(serializeElement(Random.nextLong))
  //  }

}
