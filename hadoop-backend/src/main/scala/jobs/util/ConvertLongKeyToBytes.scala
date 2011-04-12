package jobs.util

import org.apache.hadoop.io.{LongWritable, BytesWritable}
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import tasks.util.{LongKeyToBytesKey}

/**
 * User: vjovanovic
 * Date: 4/4/11
 */

object ConvertLongKeyToBytes {
  def main(args: Array[String]) {
    // parsing arguments
    val conf: Configuration = new Configuration
    val otherArgs: Array[String] = new GenericOptionsParser(conf, args).getRemainingArgs


    if (otherArgs.length != 2) {
      System.err.println("Usage: CovertLongToCollection <in> <out>")
      System.exit(2)
    }

    // setting up job classes and name
    val job: Job = new Job(conf, "ConvertLontToCollectiont")

    job.setJarByClass(classOf[LongKeyToBytesKey])
    job.setMapperClass(classOf[LongKeyToBytesKey])

    // setting input and output
    job.setInputFormatClass(classOf[SequenceFileInputFormat[LongWritable, BytesWritable]])
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[BytesWritable, BytesWritable]])
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])
    job.setOutputKeyClass(classOf[BytesWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    FileInputFormat.addInputPath(job, new Path(otherArgs(0)))
    FileOutputFormat.setOutputPath(job, new Path(otherArgs(1)))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}