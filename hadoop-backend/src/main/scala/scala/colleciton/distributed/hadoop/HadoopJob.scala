package scala.colleciton.distributed.hadoop

import java.net.URI
import org.apache.hadoop.filecache.DistributedCache
import java.util.UUID
import scala.collection.distributed.api.dag._
import scala.collection.mutable
import scala.collection.distributed.api.io.CollectionMetaData
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import org.apache.hadoop.fs.{PathFilter, FileSystem, Path}
import collection.distributed.api.{AbstractJobStrategy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.jobcontrol.Job
import org.apache.hadoop.mapred.{JobClient, JobConf}

object HadoopJob extends AbstractJobStrategy {

  def execute(dag: ExPlanDAG, globalCache: mutable.Map[String, Any]) = {
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

    JobClient.runJob(job)
    mscrBuilder.postRun(job)

    // fetch collection sizes and write them to DFS
    mscrBuilder.outputNodes.foreach(out => storeCollectionsMetaData(job, out))

    executeInternal(remainingPlan)
  }


  def storeCollectionsMetaData(job: JobConf, outputPlanNode: OutputPlanNode) = {
    //    val size = job.getCounters.findCounter("collections", "current").getValue
    //    // write metadata
    //    val metaDataPath = new Path(outputPlanNode.collection.location.toString, "META")
    //    val baos = new ByteArrayOutputStream()
    //    val oos = new ObjectOutputStream(baos)
    //    oos.writeObject(new CollectionMetaData(size))
    //    oos.flush()
    //    oos.close()
    //    FSAdapter.writeToFile(job, metaDataPath, baos.toByteArray)
  }


  private def optimizePlan(dag: ExPlanDAG): ExPlanDAG = {
    // TODO (VJ) introduce sink flattens as the only optimization for now
    dag
  }

  def dfsSerialize(conf: JobConf, key: String, data: AnyRef) = {
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
  }

  private def tmpPath(): Path = {
    new Path("tmp/" + new URI(UUID.randomUUID.toString).toString)
  }

}


class MetaPathFilter extends PathFilter {
  def accept(path: Path) = !path.toString.endsWith("META")
}