package mrapi

import java.net.URI
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}
import dcollections.api.AbstractJobStrategy
import org.apache.hadoop.filecache.DistributedCache
import java.util.UUID
import dcollections.api.dag._
import tasks.{ParallelDoMapTask, CombineTask, ParallelDoReduceTask}
import scala.collection.mutable
import dcollections.api.io.CollectionMetaData
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import org.apache.hadoop.fs.{PathFilter, FileSystem, Path}

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object HadoopJob extends AbstractJobStrategy {

  def execute(dag: ExPlanDAG, globalCache: mutable.Map[String, Any]) = {

    optimizePlan(dag)

    var queue = new mutable.Queue[PlanNode]() ++= dag.inputNodes

    while (queue.size > 0) {
      val mscrBuilder = extractMSCR(queue, dag)

      // start the job
      val config: Configuration = new Configuration
      val job = new Job(config, "TODO (VJ)")

      mscrBuilder.configure(job)

      // serialize global cache
      dfsSerialize(job, "global.cache", globalCache.toMap)

      job.waitForCompletion(true)

      // fetch collection sizes and write them to DFS
      storeCollectionsMetaData(job, mscrBuilder.output.head)

      // de-serialize global cache
      // TODO(VJ)
    }
  }

  def storeCollectionsMetaData(job: Job, outputPlanNode: OutputPlanNode) = {
    val size = job.getCounters.findCounter("collections", "current").getValue
    // write metadata
    val metaDataPath = new Path(outputPlanNode.id.location.toString, "META")
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(new CollectionMetaData(size))
    oos.flush()
    oos.close()
    FSAdapter.writeToFile(job, metaDataPath, baos.toByteArray)
  }

  def extractMSCR(queue: mutable.Queue[PlanNode], dag: ExPlanDAG): MapCombineShuffleReduceBuilder = {
    val mscrBuilder = new MapCombineShuffleReduceBuilder()
    var node: Option[PlanNode] = Some(queue.dequeue)
    var mapPhase = true
    while (node.isDefined) {
      node.get match {
        case value: InputPlanNode =>
          mscrBuilder.input += value

        case value: ParallelDoPlanNode[_, _] =>
          if (mapPhase)
            mscrBuilder.mapParallelDo = Some(value)
          else
            mscrBuilder.reduceParallelDo = Some(value)

        case value: GroupByPlanNode[_, _, _] =>
          mscrBuilder.groupBy = Some(value)
          mapPhase = false

        case value: CombinePlanNode[_, _, _, _] =>
          mscrBuilder.combine = Some(value)
          mapPhase = false

        case value: OutputPlanNode =>
          mscrBuilder.output += value

        case value: FlattenPlanNode =>
          mscrBuilder.flatten = Some(value)
          value.collections.foreach((col) => {
            // TODO (VJ) fix this cast
            val inputNode = dag.getPlanNode(col).get.asInstanceOf[InputPlanNode]
            mscrBuilder.input += inputNode
            queue.dequeueFirst(_ == inputNode)
          })
      }
      node = node.get.outEdges.headOption
    }

    mscrBuilder
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
    new Path(new URI(UUID.randomUUID.toString).toString)
  }

}

class MapCombineShuffleReduceBuilder {
  var input: mutable.Set[InputPlanNode] = mutable.HashSet()
  var mapParallelDo: Option[ParallelDoPlanNode[_, _]] = None
  var groupBy: Option[GroupByPlanNode[_, _, _]] = None
  var combine: Option[CombinePlanNode[_, _, _, _]] = None
  var reduceParallelDo: Option[ParallelDoPlanNode[_, _]] = None
  var flatten: Option[FlattenPlanNode] = None
  var output: mutable.HashSet[OutputPlanNode] = mutable.HashSet()

  def configure(job: Job) = {

    // setting mapper and reducer classes and serializing closures
    job.setMapperClass(classOf[ParallelDoMapTask])
    if (mapParallelDo.isDefined) {
      // serialize parallel do operation
      HadoopJob.dfsSerialize(job, "distcoll.mapper.do", mapParallelDo.get.parOperation)
      println("Map parallel do!!")
    }

    if (flatten.isDefined) {
      HadoopJob.dfsSerialize(job, "distcoll.mapper.flatten", flatten.get.collections)
      println("Flatten!!")
    }

    if (groupBy.isDefined) {
      // serialize group by closure
      HadoopJob.dfsSerialize(job, "distcoll.mapper.groupBy", groupBy.get.keyFunction)
      println("GroupBy!!")
    }

    // set combiner
    if (combine.isDefined) {
      HadoopJob.dfsSerialize(job, "distcoll.mapper.groupBy", combine.get.keyFunction)
      HadoopJob.dfsSerialize(job, "distcoll.mapreduce.combine", combine.get.op)
      job.setCombinerClass(classOf[CombineTask])
      println("Combine!!")
    }

    // serialize reducer parallel closure
    job.setReducerClass(classOf[ParallelDoReduceTask])
    if (reduceParallelDo.isDefined) {
      //serialize reduce parallel do
      HadoopJob.dfsSerialize(job, "distcoll.reducer.do", reduceParallelDo.get.parOperation)
      println("Reduce parallel do!!")
    }

    // setting input and output and intermediate types
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])
    job.setOutputKeyClass(classOf[BytesWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[BytesWritable, BytesWritable]])
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[BytesWritable, BytesWritable]])


    // set the input and output files for the job
    input.foreach((in) => FileInputFormat.addInputPath(job, new Path(in.id.location.toString)))
    FileInputFormat.setInputPathFilter(job: Job, classOf[MetaPathFilter])

    // TODO (VJ) fix the multiple outputs
    output.foreach((out) => FileOutputFormat.setOutputPath(job, new Path(out.id.location.toString)))
  }
}

class MetaPathFilter extends PathFilter {
  def accept(path: Path) = !path.toString.endsWith("META")
}