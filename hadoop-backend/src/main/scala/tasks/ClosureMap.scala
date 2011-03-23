package tasks

import org.apache.hadoop.mapreduce.{Mapper}
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.filecache.DistributedCache
import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/13/11
 */

class ClosureMap extends Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable] {

  // closure to be invoked
  var closure: (AnyRef) => AnyRef = null;

  override def setup(context: Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context) {
    super.setup(context)

    val conf = context.getConfiguration

    // TODO (VJ) evaluate if local cache is faster than getting the files from distributed file system.
    // find the file in the node local cache
    val closureFileURI = conf get ("closuremapper.closures")
    val cacheFiles = DistributedCache.getCacheFiles(conf);
    val closureFile = new Path(cacheFiles.filter((x: URI) => x.toString == closureFileURI)(0).toString)

    if (closureFileURI == null) {
      throw new IllegalArgumentException("Closure file is not in the properties.")
    }

    val inputStream = new java.io.ObjectInputStream(closureFile.getFileSystem(conf).open(closureFile))
    closure = inputStream.readObject().asInstanceOf[(AnyRef) => AnyRef]
    inputStream.close()
  }

  override def map(k: LongWritable, v: BytesWritable, context: Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context): Unit = {
    //deserialize element
    val bais = new ByteArrayInputStream(v.getBytes);
    val ois = new ObjectInputStream(bais);
    val value = ois.readObject()

    // apply closure
    val result = closure(value)

    //serialize again
    val baos = new ByteArrayOutputStream();
    val oos = new ObjectOutputStream(baos);
    oos.writeObject(result);
    oos.flush()

    // emit the result
    context.write(new LongWritable(result.hashCode().longValue), new BytesWritable(baos.toByteArray))
  }
}

object ClosureMap {
  val MAP_CLOSURE_FILE = "./map.closure"
}
