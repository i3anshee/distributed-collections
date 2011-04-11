package tasks

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.Reducer
import java.lang.Iterable
import scala.collection.JavaConversions._
import collection.mutable.{ArrayBuffer}
import scala.util.Random
import dcollections.api.{DistContext, RecordNumber, Emitter}
import collection.immutable
import collection.mutable

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class ParallelDoReduceTask extends Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable] with CollectionTask {

  var isGroupBy: Boolean = false
  var parTask: Option[(AnyRef, Emitter[AnyRef], DistContext) => Unit] = None
  var foldTask: Option[(AnyRef, Any) => AnyRef] = None
  var distContext: DistContext = new DistContext(mutable.Map[String, Any](), immutable.Map[String, Any]())

  override def setup(context: Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context) {
    super.setup(context)

    val conf = context.getConfiguration

    isGroupBy = conf.get("distcoll.mapper.groupBy") != null
    foldTask = deserializeOperation(conf, "distcoll.mapreduce.combine")
    parTask = deserializeOperation(conf, "distcoll.reducer.do")
  }

  override def reduce(key: BytesWritable, values: Iterable[BytesWritable], context: Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context) = {

    if (foldTask.isEmpty && parTask.isEmpty)
      if (isGroupBy) {
        // TODO fix this mess when multiple collection format is known
        // make a collection of elements and write it
        context.getCounter("collections", "current").increment(1)
        context.write(new BytesWritable(serializeElement(new Random().nextLong)),
          new BytesWritable(serializeElement((deserializeElement(key.getBytes), values.map((bytes: BytesWritable) => deserializeElement(bytes.getBytes))))))
      } else {
        values.foreach((v: BytesWritable) => {
          context.getCounter("collections", "current").increment(1)
          context.write(key, v)
        })
      }
    else {
      var buffer: Traversable[AnyRef] = values.map((v: BytesWritable) => deserializeElement(v.getBytes()))

      // combine reduce part
      if (foldTask.isDefined)
        buffer = ArrayBuffer(buffer.reduceLeft(foldTask.get))

      // parallel do
      val emitter = new EmiterImpl
      distContext.recordNumber = new RecordNumber()
      val result = parallelDo(buffer, emitter, distContext, parTask)

      // write results to output
      result.foreach((v: AnyRef) => {
        context.getCounter("collections", "current").increment(1)
        context.write(key, new BytesWritable(serializeElement(v)))
      })
    }
  }
}