package mrapi

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.{Path, FileSystem}
import java.io.ObjectOutputStream
import tasks.ClosureMap
import org.apache.hadoop.filecache.DistributedCache

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

class ClosureMapperAdapter[A, B](val closure: (A) => B) extends MapperAdapter {

  override def prepare(job: Job) = {
    val conf = job.getConfiguration

    // place the closure in distributed cache
    val fs = FileSystem get (conf)
    val hdfsPath = new Path(ClosureMap.MAP_CLOSURE_FILE)
    val hdfsos = fs.create(hdfsPath)
    val oos = new ObjectOutputStream(hdfsos)
    oos.writeObject(closure)
    oos.flush()
    oos.close()
    val serializedClosureURI = hdfsPath.toUri();
    conf.set("closuremapper.closures", serializedClosureURI.toString)
    DistributedCache.addCacheFile(serializedClosureURI, conf)

    job.setMapperClass(classOf[ClosureMap])
    job.setJarByClass(classOf[ClosureMap])
  }
}