package tasks

import org.apache.hadoop.mapreduce.Mapper
import scala.{None}
import collection.mutable.{ArrayBuffer}
import scala.collection.distributed.api.{DistContext, Emitter}
import org.apache.hadoop.io.{NullWritable, BytesWritable}

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class ParallelDoMapTask extends DistributedCollectionsMapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  // closure to be invoked
  var parTask: Option[(AnyRef, Emitter[AnyRef], DistContext) => Unit] = None
  var groupBy: Option[(AnyRef, Emitter[AnyRef]) => AnyRef] = None


  override def setup(context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context) {
    super.setup(context)

    val conf = context.getConfiguration
    parTask = deserializeOperation(conf, "distcoll.mapper.do")
    groupBy = deserializeOperation(conf, "distcoll.mapper.groupBy")
  }

  override def distMap(k: NullWritable, v: BytesWritable, context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context, distContext: DistContext): Unit = {
    //de-serialize element
    val value = deserializeElement(v.getBytes())

    distContext.globalCache.foreach(println)

    if (parTask.isEmpty && groupBy.isEmpty) {
      context.write(randomKey, v)
    } else {
      val emitter: EmitterImpl = new EmitterImpl

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
          context.write(randomKey, new BytesWritable(serializeElement(v)))
        })
      }
    }
  }
}

