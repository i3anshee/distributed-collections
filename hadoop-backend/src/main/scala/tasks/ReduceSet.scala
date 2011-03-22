package tasks

import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.Reducer
import java.lang.Iterable
import scala.collection.JavaConversions._

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

class ReduceSet extends Reducer[LongWritable, BytesWritable, LongWritable, BytesWritable] {

  override def reduce(key: LongWritable, values: Iterable[BytesWritable], context: Reducer[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context) = {
    // eliminate duplicates
    val valuesSet = values.toSet

    // for now
    valuesSet.foreach (context.write(key, _))
  }

}