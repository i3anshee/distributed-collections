package scala.colleciton.distributed.hadoop

import java.net.URI
import org.apache.hadoop.filecache.DistributedCache
import java.util.UUID
import scala.collection.distributed.api.dag._
import scala.collection.mutable
import java.io.ObjectOutputStream
import org.apache.hadoop.fs.{PathFilter, FileSystem, Path}
import collection.distributed.api.AbstractJobStrategy
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{JobClient, JobConf}

object HadoopJob extends AbstractJobStrategy {

  def execute(dag: ExPlanDAG) = {
    val optimizedDag = optimizePlan(dag)
    executeInternal(optimizedDag)
  }

  private def executeInternal(dag: ExPlanDAG): Unit = if (!dag.isEmpty) {

    // extract first builder and dag
    val mscrBuilder = new GreedyMSCRBuilder
    val remainingPlan = mscrBuilder.build(dag)

    // TODO (VJ) remove
    println("\n\nmapperGraph => " + mscrBuilder.mapDAG.toString)
    println("\n\reducerGraph => " + mscrBuilder.reduceDAG.toString)

    // execute builder
    val config: Configuration = new Configuration
    val job = new JobConf(config)

    mscrBuilder.configure(job)

    val runningJob = JobClient.runJob(job)

    mscrBuilder.postRun(job, runningJob)

    executeInternal(remainingPlan)
  }

  private def optimizePlan(dag: ExPlanDAG): ExPlanDAG = {
    dag
  }

  def dfsSerialize(conf: JobConf, key: String, data: AnyRef): URI = {
    // place the closure in distributed cache
    val fs = FileSystem.get(conf)
    val hdfsPath = tmpPath()
    val hdfsos = fs.create(hdfsPath)
    val oos = new ObjectOutputStream(hdfsos)
    oos.writeObject(data)
    oos.flush()
    oos.close()

    val serializedDataURI = hdfsPath.toUri();
    conf.set(key, serializedDataURI.toString)
    DistributedCache.addCacheFile(serializedDataURI, conf)
    serializedDataURI
  }

  private def tmpPath(): Path = {
    new Path("tmp/" + new URI(UUID.randomUUID.toString).toString)
  }

}


class MetaPathFilter extends PathFilter {
  def accept(path: Path) = !path.toString.endsWith("META")
}