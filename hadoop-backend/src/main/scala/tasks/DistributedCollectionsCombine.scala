package tasks

import org.apache.hadoop.io.BytesWritable
import java.lang.Iterable
import scala.collection.JavaConversions._
import java.util.Iterator
import org.apache.hadoop.mapred._

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class DistributedCollectionsCombine extends MapReduceBase with Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable] with CollectionTask {


//  var foldTask: Option[(Any, Any) => Any] = None
//
//  override def setup(context: Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context) {
//    super.setup(context)
//
//    val conf = context.getConfiguration
//
//    foldTask = deserializeOperation(conf, "distcoll.mapreduce.combine")
//    if (foldTask.isEmpty) throw new RuntimeException("Combine operation must be declared !!!!")
//  }
//
//  override def reduce(key: BytesWritable, values: Iterable[BytesWritable], context: Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context) = {
//    // combine reduce part
//    val buffer: Traversable[AnyRef] = values.map((v: BytesWritable) => deserializeElement(v.getBytes()))
//    context.write(key, new BytesWritable(serializeElement(buffer.reduceLeft(foldTask.get))))
//  }

  override def close = {}

  override def configure(job: JobConf) = {}

  def reduce(key: BytesWritable, values: Iterator[BytesWritable], output: OutputCollector[BytesWritable, BytesWritable], reporter: Reporter) = {}
}