package tasks

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.BytesWritable
import java.lang.Iterable
import scala.collection.JavaConversions._


/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class CombineTask extends Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable] {

  override def reduce(key: BytesWritable, values: Iterable[BytesWritable], context: Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context) = {
  }

}