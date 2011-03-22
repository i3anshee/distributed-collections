package mrapi

import java.net.URI
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, BytesWritable}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object JobAdapter {

  /**
   * For now a simple 1 map 1 reduce. Later we will extend it to multiple input multiple output job.
   */
  def execute(inputFile: URI, mapper: MapperAdapter, reducer: ReducerAdapter, outputFile: URI): Unit = {
    val conf: Configuration = new Configuration
    val job: Job = new Job(conf, "ClosureMapJob")

    // setting mapper and reducer
    mapper.prepare(job)
    reducer.prepare(job)

    // setting input and output and intermediate types
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])
    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    job.setInputFormatClass(classOf[SequenceFileInputFormat[LongWritable, BytesWritable]])
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[LongWritable, BytesWritable]])

    FileInputFormat.addInputPath(job, new Path(inputFile.toString))
    FileOutputFormat.setOutputPath(job, new Path(outputFile.toString))

    job.waitForCompletion(true)
  }

}