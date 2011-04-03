package tasks.util

import org.apache.hadoop.io.{LongWritable, BytesWritable}
import org.apache.hadoop.mapreduce.Mapper
import tasks.CollectionTask

/**
 * User: vjovanovic
 * Date: 4/4/11
 */

class LongKeyToBytesKey extends Mapper[LongWritable, BytesWritable, BytesWritable, BytesWritable] with CollectionTask {
  override def map(k: LongWritable, v: BytesWritable, context: Mapper[LongWritable, BytesWritable, BytesWritable, BytesWritable]#Context): Unit = {
    context.write(
      new BytesWritable(serializeElement(k.get)), v)
  }
}