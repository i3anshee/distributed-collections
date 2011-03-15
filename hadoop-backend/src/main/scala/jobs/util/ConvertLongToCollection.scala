package jobs.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.{Mapper, Job}
import tasks.util.IdentityLongToBytesReducer
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}

/**
 * Utility Class for creating ByteWritable collections from LongWritable ones. Currently used for Closure testing.
 *
 * User: vjovanovic
 * Date: 3/13/11
 */
object ConvertLongToCollection {

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

    job.setJarByClass(classOf[IdentityLongToBytesReducer])
    job.setMapperClass(classOf[Mapper[LongWritable, LongWritable, LongWritable, LongWritable]])
    job.setReducerClass(classOf[IdentityLongToBytesReducer])

    // setting input and output
    job.setInputFormatClass(classOf[SequenceFileInputFormat[LongWritable, LongWritable]])
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[LongWritable, BytesWritable]])
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[LongWritable])
    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    FileInputFormat.addInputPath(job, new Path(otherArgs(0)))
    FileOutputFormat.setOutputPath(job, new Path(otherArgs(1)))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}

