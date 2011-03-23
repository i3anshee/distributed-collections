package mrapi

import org.apache.hadoop.mapreduce.Job
import java.io.ObjectOutputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import tasks.{ClosureReduce, ClosureReducer}
import org.apache.hadoop.filecache.DistributedCache

/**
 * User: vjovanovic
 * Date: 3/23/11
 */

class ReduceClosureAdapter[A, B](val closureOp: (A, B) => B) extends ReducerAdapter {
  def prepare(job: Job) = {
    // serialize closureOp operation
    val conf = job.getConfiguration

    // place the closure in distributed cache
    val fs = FileSystem get (conf)
    val hdfsPath = new Path(ClosureReduce.REDUCE_CLOSURE_FILE)
    val hdfsos = fs.create(hdfsPath)
    val oos = new ObjectOutputStream(hdfsos)
    oos.writeObject(closureOp)
    oos.flush()
    oos.close()
    val serializedClosureURI = hdfsPath.toUri();
    conf.set("closurereducer.closures", serializedClosureURI.toString)
    DistributedCache.addCacheFile(serializedClosureURI, conf)

    // set the reducer for the job
    job.setReducerClass(classOf[ClosureReducer])
//    job.setCombinerClass(classOf[ClosureReducer])
  }
}