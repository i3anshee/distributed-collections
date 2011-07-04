package tasks

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import collection.distributed.api.io.SerializerInstance
import io.KryoSerializer

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

trait CollectionTask {
  var serializerInstance: SerializerInstance = new KryoSerializer().newInstance()

  def deserializeFromCache[T](conf: Configuration, name: String): Option[T] = {
    // find the file in the node local cache
    val opFileURI = conf.get(name)
    if (opFileURI == null)
      None
    else {
      val cacheFiles = DistributedCache.getCacheFiles(conf)
      val opFile = new Path(cacheFiles.filter((x: URI) => x.toString == opFileURI)(0).toString)
      val inputStream = new java.io.ObjectInputStream(opFile.getFileSystem(conf).open(opFile))
      val result = Some(inputStream.readObject().asInstanceOf[T])
      inputStream.close()
      result
    }
  }
}