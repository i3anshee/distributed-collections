package tasks

import org.apache.hadoop.mapreduce.{Mapper}
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.filecache.DistributedCache

/**
 * User: vjovanovic
 * Date: 3/13/11
 */

class ClosureMap extends Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable] {

  // closure to be invoked
  var closure: (AnyRef) => AnyRef = null;

  override def setup(context: Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context) {
    super.setup(context)

    // find the file in the node local cache
    val conf = context.getConfiguration
    val cacheFiles = DistributedCache.getLocalCacheFiles(conf);

    var closureFile: Path = null;
    for (i <- 0 to (cacheFiles.length - 1)) {
      if (cacheFiles(i) != null && cacheFiles(i).getName.endsWith(ClosureMap.MAP_CLOSURE_FILE)) {
        closureFile = cacheFiles(i)
      }
    }

    if (closureFile == null) {
      throw new IllegalArgumentException("Closure file is not in the Local File cache.")
    }

    val inputStream = new java.io.ObjectInputStream(closureFile.getFileSystem(conf).open(closureFile))
    closure = inputStream.readObject().asInstanceOf[(AnyRef) => AnyRef]
    inputStream.close();
  }

  override def map(k: LongWritable, v: BytesWritable, context: Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context): Unit = {
    //deserialize element
    val bais = new ByteArrayInputStream(v.getBytes);
    val ois = new ObjectInputStream(bais);
    val value = ois.readObject()

    // apply closure
    val result = closure(v)

    //serialize again
    val baos = new ByteArrayOutputStream();
    val oos = new ObjectOutputStream(baos);
    oos.writeObject(result);
    oos.flush

    // emit the result
    context write (k, new BytesWritable(baos.toByteArray))
  }
}

object ClosureMap {
  val MAP_CLOSURE_FILE = "./map.closure"
}
