package tasks

import org.apache.hadoop.mapreduce.Reducer
import java.lang.Iterable
import scala.collection.JavaConversions._
import collection.mutable.{ArrayBuffer}
import scala.util.Random
import scala.collection.distributed.api.{DistContext, RecordNumber, Emitter}
import collection.immutable
import collection.mutable
import org.apache.hadoop.io.{NullWritable, BytesWritable}

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class ParallelDoReduceTask extends Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] with CollectionTask {

  var isGroupBy: Boolean = false
  var parTask: Option[(AnyRef, Emitter[AnyRef], DistContext) => Unit] = None
  var foldTask: Option[(AnyRef, Any) => AnyRef] = None
  var distContext: DistContext = new DistContext(immutable.Map[String, Any]())

  override def setup(context: Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable]#Context) {
    super.setup(context)

    val conf = context.getConfiguration

    isGroupBy = conf.get("distcoll.mapper.groupBy") != null
    foldTask = deserializeOperation(conf, "distcoll.mapreduce.combine")
    parTask = deserializeOperation(conf, "distcoll.reducer.do")
  }

  override def reduce(key: BytesWritable, values: Iterable[BytesWritable], context: Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable]#Context) = {

    if (foldTask.isEmpty && parTask.isEmpty)
      if (isGroupBy) {
        // TODO fix this mess when multiple collection format is known
        // make a collection of elements and write it
        context.getCounter("collections", "current").increment(1)
        context.write(NullWritable.get, new BytesWritable(serializeElement(
          (deserializeElement(key.getBytes), values.map((bytes: BytesWritable) => deserializeElement(bytes.getBytes))))))
      } else {
        values.foreach((v: BytesWritable) => {
          context.getCounter("collections", "current").increment(1)
          context.write(NullWritable.get, v)
        })
      }
    else {
      var buffer: Traversable[AnyRef] = values.map((v: BytesWritable) => deserializeElement(v.getBytes()))

      // combine reduce part
      if (foldTask.isDefined) {
        buffer = ArrayBuffer(buffer.reduceLeft(foldTask.get))
        // write results to output
        buffer.foreach((v: AnyRef) => {
          context.getCounter("collections", "current").increment(1)
          context.write(NullWritable.get,
            new BytesWritable(serializeElement((deserializeElement(key.getBytes), v))))
        })
      } else {
        // parallel do
        val emitter = new EmiterImpl
        distContext.recordNumber = new RecordNumber()
        val result = parallelDo(buffer, emitter, distContext, parTask)

        // write results to output
        result.foreach((v: AnyRef) => {
          context.getCounter("collections", "current").increment(1)
          context.write(NullWritable.get, new BytesWritable(serializeElement(v)))
        })
      }
    }
  }
}