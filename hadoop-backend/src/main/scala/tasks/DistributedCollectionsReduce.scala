package tasks

import collection.distributed.api.DistContext
import collection.distributed.api.dag.ExPlanDAG
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import java.util.Iterator
import org.apache.hadoop.mapred._

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class DistributedCollectionsReduce extends MapReduceBase with Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] with CollectionTask {

  var distContext: DistContext = null
  var reduceDAG: ExPlanDAG = null

  override def configure(job: JobConf) = {
    super.configure(job)

    reduceDAG = deserializeFromCache(job, "distribted-collections.reduceDAG").get
  }

  override def close = {
  }

  def reduce(key: BytesWritable, values: Iterator[BytesWritable], output: OutputCollector[NullWritable, BytesWritable], reporter: Reporter) = {}
}

//  var isGroupBy: Boolean = false
//  var parTask: Option[(AnyRef, Emitter[AnyRef], DistContext) => Unit] = None
//  var foldTask: Option[(AnyRef, Any) => AnyRef] = None
//  var distContext: DistContext = new DistContext(immutable.Map[String, Any]())
//
//  override def setup(context: Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable]#Context) {
//    super.setup(context)
//
//    val conf = context.getConfiguration
//
//    isGroupBy = conf.get("distcoll.mapper.groupBy") != null
//    foldTask = deserializeOperation(conf, "distcoll.mapreduce.combine")
//    parTask = deserializeOperation(conf, "distcoll.reducer.do")
//  }
//
//  override def reduce(key: BytesWritable, values: Iterable[BytesWritable], context: Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable]#Context) = {
//
//    if (foldTask.isEmpty && parTask.isEmpty)
//      if (isGroupBy) {
//        // TODO fix this mess when multiple collection format is known
//        // make a collection of elements and write it
//        context.getCounter("collections", "current").increment(1)
//        context.write(NullWritable.get, new BytesWritable(serializeElement(
//          (deserializeElement(key.getBytes), values.map((bytes: BytesWritable) => deserializeElement(bytes.getBytes))))))
//      } else {
//        values.foreach((v: BytesWritable) => {
//          context.getCounter("collections", "current").increment(1)
//          context.write(NullWritable.get, v)
//        })
//      }
//    else {
//      var buffer: Traversable[AnyRef] = values.map((v: BytesWritable) => deserializeElement(v.getBytes()))
//
//      // combine reduce part
//      if (foldTask.isDefined) {
//        buffer = ArrayBuffer(buffer.reduceLeft(foldTask.get))
//        // write results to output
//        buffer.foreach((v: AnyRef) => {
//          context.getCounter("collections", "current").increment(1)
//          context.write(NullWritable.get,
//            new BytesWritable(serializeElement((deserializeElement(key.getBytes), v))))
//        })
//      } else {
//        // parallel do
////        val emitter = new EmitterImpl
////        distContext.recordNumber = new RecordNumber()
////        val result = parallelDo(buffer, emitter, distContext, parTask)
////
////        // write results to output
////        result.foreach((v: AnyRef) => {
////          context.getCounter("collections", "current").increment(1)
////          context.write(NullWritable.get, new BytesWritable(serializeElement(v)))
////        })
//      }
//    }
//  }
//}