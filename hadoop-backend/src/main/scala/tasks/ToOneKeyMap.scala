package tasks

import org.apache.hadoop.io.{LongWritable, BytesWritable}
import org.apache.hadoop.mapreduce.Mapper

/**
 * User: vjovanovic
 * Date: 3/23/11
 */

class ToOneKeyMap extends Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable] {

  override def map(k: LongWritable, v: BytesWritable, context: Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context): Unit = {
    // TODO (VJ) find the mapper unique id
    context.write(new LongWritable(1), v)
  }
}