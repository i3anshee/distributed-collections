package jobs.util

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configuration
import tasks.ClosureMap
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.ObjectOutputStream
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.filecache.DistributedCache

/**
 * Example of ClosureMap usage on any type of data.
 *
 * User: vjovanovic
 * Date: 3/15/11
 */

object ClosureMapJob {

  def main(args: Array[String]) {
    // parsing arguments
    val conf: Configuration = new Configuration
    val otherArgs: Array[String] = new GenericOptionsParser(conf, args).getRemainingArgs

    if (otherArgs.length != 2) {
      System.err.println("Usage: ClosureMapJob <in> <out>")
      System.exit(2)
    }

    val closure = (x: Int) => x + 1

    // place the closure in distributed cache
    val fs = FileSystem get (conf)
    val hdfsPath = new Path(ClosureMap.MAP_CLOSURE_FILE)
    val hdfsos = fs create (hdfsPath)
    val oos = new ObjectOutputStream(hdfsos)
    oos writeObject (closure)
    oos flush ()
    oos close ()
    DistributedCache.addCacheFile(hdfsPath.toUri(), conf)

    // setting up job classes and name
    val job: Job = new Job(conf, "ClosureMapJob")
    job setJarByClass (classOf[ClosureMap])
    job setMapperClass (classOf[ClosureMap])

    // setting input and output
    job setInputFormatClass (classOf[SequenceFileInputFormat[LongWritable, BytesWritable]])
    job setMapOutputKeyClass (classOf[LongWritable])
    job setMapOutputValueClass (classOf[BytesWritable])
    job setOutputKeyClass (classOf[LongWritable])
    job setOutputValueClass (classOf[BytesWritable])

    FileInputFormat addInputPath (job, new Path(otherArgs(0)))
    FileOutputFormat setOutputPath (job, new Path(otherArgs(1)))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}