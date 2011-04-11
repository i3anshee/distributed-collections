package tasks

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.{BytesWritable}
import scala.{None}
import collection.mutable.{Buffer, ArrayBuffer}
import dcollections.api.{DistContext, RecordNumber, Emitter}

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class ParallelDoMapTask extends DistributedCollectionsMapper[BytesWritable, BytesWritable, BytesWritable, BytesWritable] {
  // closure to be invoked
  var parTask: Option[(AnyRef, Emitter[AnyRef], DistContext) => Unit] = None
  var groupBy: Option[(AnyRef, Emitter[AnyRef]) => AnyRef] = None


  override def setup(context: Mapper[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context) {
    super.setup(context)

    val conf = context.getConfiguration
    parTask = deserializeOperation(conf, "distcoll.mapper.do")
    groupBy = deserializeOperation(conf, "distcoll.mapper.groupBy")
  }

  override def distMap(k: BytesWritable, v: BytesWritable, context: Mapper[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context, distContext: DistContext): Unit = {
    //de-serialize element
    val value = deserializeElement(v.getBytes())

    distContext.globalCache.foreach(println)

    if (parTask.isEmpty && groupBy.isEmpty) {
      context.write(k, v)
    } else {
      val emitter: EmiterImpl = new EmiterImpl

      // apply parallel do
      val emitted = parallelDo(ArrayBuffer(value), emitter, distContext, parTask)

      // apply group by
      if (groupBy.isDefined) {
        emitted.foreach((el) => {
          val key = groupBy.get(el, emitter)
          emitter.getBuffer.foreach((v: AnyRef) => {
            context.write(new BytesWritable(serializeElement(key)), new BytesWritable(serializeElement(v)))
          })
          emitter.clear
        })
      } else {
        emitted.foreach((v: AnyRef) => {
          context.write(k, new BytesWritable(serializeElement(v)))
        })
      }
    }
  }

}

