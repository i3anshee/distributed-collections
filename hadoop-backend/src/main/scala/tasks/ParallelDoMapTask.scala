package tasks

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.io.{BytesWritable}
import dcollections.api.Emitter

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class ParallelDoMapTask extends Mapper[BytesWritable, BytesWritable, BytesWritable, BytesWritable] with CollectionTask {
  // closure to be invoked
  var task: (AnyRef, Emitter[AnyRef]) => AnyRef = None
  var groupBy: Option[(AnyRef) => AnyRef] = None

  var taskDefined: Boolean
  var groupByDefined: Boolean

  override def setup(context: Mapper[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context) {
    super.setup(context)

    val conf = context.getConfiguration

    // find the file in the node local cache
    val closureFileURI = conf.get("distcoll.mapper.")
    val cacheFiles = DistributedCache.getCacheFiles(conf)
    val closureFile = new Path(cacheFiles.filter((x: URI) => x.toString == closureFileURI)(0).toString)

    if (closureFileURI == null) {
      throw new IllegalArgumentException("Closure file is not in the properties.")
    }

    val inputStream = new java.io.ObjectInputStream(closureFile.getFileSystem(conf).open(closureFile))
    task = inputStream.readObject().asInstanceOf[(AnyRef) => Boolean]
    inputStream.close()
  }

  override def map(k: BytesWritable, v: BytesWritable, context: Mapper[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context): Unit = {
    //deserialize element
    val value = deserializeElement(v.getBytes())

    // apply closure
    if (taskDefined) {
      task(value)
    }
  }
}