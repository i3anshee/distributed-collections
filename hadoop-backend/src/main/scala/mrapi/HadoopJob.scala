package mrapi

import _root_.execution.dag._
import java.net.URI
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, BytesWritable}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}
import dcollections.api.AbstractJobStrategy
import dcollections.api.dag.{GroupByPlanNode, ExPlanDAG}
import tasks.{ClosureCombineTask, ParallelDoReduceTask}

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object HadoopJob extends AbstractJobStrategy {

  def execute(dag: ExPlanDAG) = {
    val queue = dag.inputNodes

    val mscrBuilder = new MapCombineShuffleReduceBuilder()

    // capture inputs
    null
  }
}

class MapCombineShuffleReduceBuilder {
  var input: Option[InputPlanNode]
  var mapParalelDo: Option[ParallelDoPlanNode] = None
  var groupBy: Option[GroupByPlanNode] = None
  var combine: Option[CombinePlanNode] = None
  var reduceParallelDo: Option[ParallelDoPlanNode] = None
  var output: Option[OutputPlanNode]

  def configure(job: Job) = {
    val conf: Configuration = new Configuration
    val job: Job = new Job(conf, "TODO (VJ)")

    // setting mapper and reducer classes and serializing closures
    if (mapParalelDo.isDefined) {
      // serialize parallel do operation
      dfsSerialize("distcoll.mapper.do", mapParalelDo.get.parOperation)
    }

    if (groupBy.isDefined) {
      // serialize group by closure
      dfsSerialize("distcoll.mapper.do", groupBy.get.keyFunction)
    }

    // set combiner
    if (combine.isDefined) {
      dfsSerialize("distcoll.mapper.do", combine.get.keyFunction)
      dfsSerialize("distcoll.mapper.combine", combine.get.op)
      job.setCombinerClass(classOf[ClosureCombineTask])
    }

    // serialize combiner closure
    job.setReducerClass(classOf[ParallelDoReduceTask])
    if (reduceParallelDo.isDefined) {
      //serialize reduce parallel do
      dfsSerialize("distcoll.reducer.do", reduceParallelDo.get.parOperation)
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
    FileInputFormat.addInputPath(job, new Path(input.get.uri))
    FileOutputFormat.setOutputPath(job, new Path(output.get.uri))
    //
    job.waitForCompletion(true)
  }

  private def dfsSerialize(key: String, data: AnyRef) {
    // serialize
  }
}