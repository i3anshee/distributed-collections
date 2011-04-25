package mrapi;

import org.apache.hadoop.mapred.SequenceFileInputFilter;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import tasks.CombineTask;
import tasks.ParallelDoMapTask;
import tasks.ParallelDoReduceTask;

import java.io.IOException;

/**
 * User: vjovanovic
 * Date: 4/23/11
 */
public class QuickTypeFixScalaI0 {
    static void setJobClassesBecause210SnapshotWillNot(Job job, boolean setCombiner) throws IOException {
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(ParallelDoMapTask.class);
        job.setReducerClass(ParallelDoReduceTask.class);

        if (setCombiner) {
            job.setCombinerClass(CombineTask.class);
        }
    }
}
