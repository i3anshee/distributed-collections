package tasks

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.fs.Path
import java.net.URI
import java.io.{ObjectOutputStream, ByteArrayOutputStream, ObjectInputStream, ByteArrayInputStream}
import org.apache.hadoop.filecache.DistributedCache

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

class ClosureFilterMap extends Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable] {
  // closure to be invoked
  var predicateClosure: (AnyRef) => Boolean = null;

  override def setup(context: Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context) {
    super.setup(context)

    val conf = context.getConfiguration

    // find the file in the node local cache
    val closureFileURI = conf.get("closuremapper.closures")
    val cacheFiles = DistributedCache.getCacheFiles(conf)
    val closureFile = new Path(cacheFiles.filter((x: URI) => x.toString == closureFileURI)(0).toString)

    if (closureFileURI == null) {
      throw new IllegalArgumentException("Closure file is not in the properties.")
    }

    val inputStream = new java.io.ObjectInputStream(closureFile.getFileSystem(conf).open(closureFile))
    predicateClosure = inputStream.readObject().asInstanceOf[(AnyRef) => Boolean]
    inputStream.close()
  }

  override def map(k: LongWritable, v: BytesWritable, context: Mapper[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context): Unit = {
    //deserialize element
    val bais = new ByteArrayInputStream(v.getBytes);
    val ois = new ObjectInputStream(bais);
    val value = ois.readObject()

    // apply closure
    if (predicateClosure(value)) {

      //serialize again
      val baos = new ByteArrayOutputStream();
      val oos = new ObjectOutputStream(baos);
      oos.writeObject(value);
      oos.flush()


      // TODO(VJ) introduce strategy(set, map, seq) for output processing of key value pairs that is loaded from distributed cache
      // introducing it to the class hierarchy would be an overkill

      // emit the result (set strategy)
      context.write(new LongWritable(value.hashCode().longValue), new BytesWritable(baos.toByteArray))
    }
  }
}