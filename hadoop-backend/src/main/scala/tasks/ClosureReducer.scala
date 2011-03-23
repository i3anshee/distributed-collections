package tasks

import org.apache.hadoop.io.{LongWritable, BytesWritable}
import java.io.{ObjectOutputStream, ByteArrayOutputStream, ByteArrayInputStream, ObjectInputStream}
import org.apache.hadoop.mapreduce.Reducer
import java.lang.Iterable
import scala.collection.JavaConversions._
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.Path
import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/23/11
 */

class ClosureReducer extends Reducer[LongWritable, BytesWritable, LongWritable, BytesWritable] {

  var closure: (AnyRef, AnyRef) => AnyRef = null

  override def setup(context: Reducer[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context) = {
    super.setup(context)

    val conf = context.getConfiguration

    // TODO (VJ) evaluate if local cache is faster than getting the files from distributed file system.
    // find the file in the node local cache
    val closureFileURI = conf get ("closurereducer.closures")
    val cacheFiles = DistributedCache.getCacheFiles(conf);
    val closureFile = new Path(cacheFiles.filter((x: URI) => x.toString == closureFileURI)(0).toString)

    if (closureFileURI == null) {
      throw new IllegalArgumentException("Closure file is not in the properties.")
    }

    val inputStream = new java.io.ObjectInputStream(closureFile.getFileSystem(conf).open(closureFile))
    closure = inputStream.readObject().asInstanceOf[(AnyRef, AnyRef) => AnyRef]
    inputStream.close()
  }

  override def reduce(key: LongWritable, values: Iterable[BytesWritable],
                      context: Reducer[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context) = {

    // create the collection out of the bytes writable (mappers make sure that the collection is reasonably small to fit into memory
    val mapResults: scala.collection.Iterable[AnyRef] = values.map((bytes: BytesWritable) => {
      val bais = new ByteArrayInputStream(bytes.getBytes)
      val ois = new ObjectInputStream(bais)
      ois.readObject().asInstanceOf[AnyRef]
    })

    // apply the closure
    val result = mapResults.reduceLeft(closure)

    //serialize result
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(result)
    oos.flush()

    // reduce it and write as single output
    context.write(key, new BytesWritable(baos.toByteArray))
  }

}

object ClosureReduce {
  val REDUCE_CLOSURE_FILE = "reduce.closures"
}
