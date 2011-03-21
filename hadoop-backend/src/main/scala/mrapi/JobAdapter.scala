package mrapi

import java.net.URI
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, BytesWritable}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}

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
    //    val serializedClosureUri = hdfsPath.toUri ();
    //    conf set ("closuremapper.closures", serializedClosureUri.toString)
    //    DistributedCache.addCacheFile (serializedClosureUri, conf)
    //
    //    // setting up job classes and name
    val job: Job = new Job(conf, "ClosureMapJob")
    mapper.prepare(job)
    if (reducer != null) reducer.prepare(job)

    // setting input and output and intermediate types
    job.setInputFormatClass(classOf[SequenceFileInputFormat[LongWritable, BytesWritable]])
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])
    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    FileInputFormat.addInputPath(job, new Path(inputFile.toString))
    FileOutputFormat.setOutputPath(job, new Path(outputFile.toString))

    job.waitForCompletion(true)
  }

}