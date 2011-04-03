package mrapi

import java.net.URI
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}
import dcollections.api.AbstractJobStrategy
import tasks.{CombineTask, ParallelDoReduceTask}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.ObjectOutputStream
import org.apache.hadoop.filecache.DistributedCache
import java.util.UUID
import dcollections.api.dag._

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object HadoopJob extends AbstractJobStrategy {

  def execute(dag: ExPlanDAG) = {

    optimizePlan(dag)

    val queue = new scala.collection.mutable.Queue[PlanNode]() ++= dag.inputNodes

    while (queue.size > 0) {
      val mscrBuilder = new MapCombineShuffleReduceBuilder()
      var node: Option[PlanNode] = Some(queue.dequeue)
      var mapPhase = true
      while (node.isDefined) {
        node.get match {
          case value: InputPlanNode =>
            mscrBuilder.input = Some(value)
          case value: ParallelDoPlanNode[_, _] =>
            if (mapPhase)
              mscrBuilder.mapParallelDo = Some(value)
            else
              mscrBuilder.reduceParallelDo = Some(value)
          case value: GroupByPlanNode[_, _, _] =>
            mscrBuilder.groupBy = Some(value)
            mapPhase = false
          case value: CombinePlanNode[_, _, _] =>
            mscrBuilder.combine = Some(value)
            mapPhase = false
          case value: OutputPlanNode =>
            mscrBuilder.output = Some(value)
        }
        node = node.get.outEdges.headOption
      }

      // start the job
      val conf: Configuration = new Configuration
      val job: Job = new Job(conf, "TODO (VJ)")

      mscrBuilder.configure(job)
      job.waitForCompletion(true)
    }
  }

  private def optimizePlan(dag: ExPlanDAG): ExPlanDAG = {
    // TODO (VJ) introduce optimizations
    dag
  }

}

class MapCombineShuffleReduceBuilder {
  var input: Option[InputPlanNode] = None
  var mapParallelDo: Option[ParallelDoPlanNode[_, _]] = None
  var groupBy: Option[GroupByPlanNode[_, _, _]] = None
  var combine: Option[CombinePlanNode[_, _, _]] = None
  var reduceParallelDo: Option[ParallelDoPlanNode[_, _]] = None
  var output: Option[OutputPlanNode] = None

  def configure(job: Job) = {
    // setting mapper and reducer classes and serializing closures
    if (mapParallelDo.isDefined) {
      // serialize parallel do operation
      dfsSerialize(job, "distcoll.mapper.do", mapParallelDo.get.parOperation)
    }

    if (groupBy.isDefined) {
      // serialize group by closure
      dfsSerialize(job, "distcoll.mapper.groupBy", groupBy.get.keyFunction)
    }

    // set combiner
    if (combine.isDefined) {
      dfsSerialize(job, "distcoll.mapper.do", combine.get.keyFunction)
      dfsSerialize(job, "distcoll.mapreduce.combine", combine.get.op)
      job.setCombinerClass(classOf[CombineTask])
    }

    // serialize reducer parallel closure
    job.setReducerClass(classOf[ParallelDoReduceTask])
    if (reduceParallelDo.isDefined) {
      //serialize reduce parallel do
      dfsSerialize(job, "distcoll.reducer.do", reduceParallelDo.get.parOperation)
    }

    // setting input and output and intermediate types
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])
    job.setOutputKeyClass(classOf[BytesWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    //
    job.setInputFormatClass(classOf[SequenceFileInputFormat[BytesWritable, BytesWritable]])
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[BytesWritable, BytesWritable]])

    // set the input and output files for the job
    FileInputFormat.addInputPath(job, new Path(input.get.uri.toString))
    FileOutputFormat.setOutputPath(job, new Path(output.get.uri.toString))
  }

  private def dfsSerialize(job: Job, key: String, data: AnyRef) = {
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