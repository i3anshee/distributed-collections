package tasks.util

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.{Reducer}
import scala.collection.JavaConversions._

/**
 * Utility Class for creating ByteWritable collections from LongWritable ones. Currently used for Closure testing.
 *
 * User: vjovanovic
 * Date: 3/13/11
 */
class IdentityLongToBytesReducer extends Reducer[LongWritable, LongWritable, LongWritable, BytesWritable] {

  override def reduce(key: LongWritable, values: java.lang.Iterable[LongWritable],
                      context: Reducer[LongWritable, LongWritable, LongWritable, BytesWritable]#Context): Unit = {

    values foreach ((value:LongWritable) => {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos writeObject (value)
      oos flush ()
      context write (key, new BytesWritable(baos.toByteArray))
    })
  }
}

