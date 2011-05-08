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
import org.apache.hadoop.mapreduce.Job


object HadoopJob extends AbstractJobStrategy {

  def execute(dag: ExPlanDAG, globalCache: mutable.Map[String, Any]) = {
    val optimizedDag = optimizePlan(dag)
    executeInternal(optimizedDag)
  }

  private def executeInternal(dag: ExPlanDAG) = {

    // extract first builder and dag
    val mscrBuilder = new GreedyMSCRBuilder
    val remainingExPlan = mscrBuilder.build(dag)

    // TODO (VJ) remove
    println("\n\nmapperGraph => " + mscrBuilder.mapperDAG.toString)
    println("\n\reducerGraph => " + mscrBuilder.reduceDAG.toString)

    // execute builder
    val config: Configuration = new Configuration
    val job = new Job(config, "TODO (VJ)")

    mscrBuilder.configure(job)

    // serialize global cache
//     dfsSerialize(job, "global.cache", globalCache.toMap)

    //    job.waitForCompletion(true)

    // fetch collection sizes and write them to DFS
    //    mscrBuilder.output.foreach(out => storeCollectionsMetaData(job, out))

    // de-serialize global cache
    // TODO(VJ)

    // recurse over rest of the dag
    //    executeInternal(remainingExPlan)
  }


  def storeCollectionsMetaData(job: Job, outputPlanNode: OutputPlanNode) = {
    val size = job.getCounters.findCounter("collections", "current").getValue
    // write metadata
    val metaDataPath = new Path(outputPlanNode.collection.location.toString, "META")
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(new CollectionMetaData(size))
    oos.flush()
    oos.close()
    FSAdapter.writeToFile(job, metaDataPath, baos.toByteArray)
  }


  private def optimizePlan(dag: ExPlanDAG): ExPlanDAG = {
    // TODO (VJ) introduce optimizations
    dag
  }

  def dfsSerialize(job: Job, key: String, data: AnyRef) = {
    // place the closure in distributed cache
    val conf = job.getConfiguration
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