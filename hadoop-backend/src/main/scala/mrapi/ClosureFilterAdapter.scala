package mrapi

import org.apache.hadoop.mapreduce.Job
import java.io.ObjectOutputStream
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.filecache.DistributedCache
import tasks.{ClosureMap, ClosureFilterMap}

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

class ClosureFilterAdapter[A](predicate: (A => Boolean)) extends MapperAdapter {
  override def prepare(job: Job) = {
    val conf = job.getConfiguration

    // place the closure in distributed cache
    val fs = FileSystem.get(conf)
    val hdfsPath = new Path(ClosureMap.MAP_CLOSURE_FILE)
    val hdfsos = fs.create(hdfsPath)
    val oos = new ObjectOutputStream(hdfsos)
    oos.writeObject(predicate)
    oos.flush()
    oos.close()
    val serializedClosureURI = hdfsPath.toUri();
    conf.set("closuremapper.closures", serializedClosureURI.toString)
    DistributedCache.addCacheFile(serializedClosureURI, conf)

    job.setMapperClass(classOf[ClosureFilterMap])
  }
}