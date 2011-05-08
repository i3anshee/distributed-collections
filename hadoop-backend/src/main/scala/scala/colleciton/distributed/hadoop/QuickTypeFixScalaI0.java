package scala.colleciton.distributed.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import scala.collection.distributed.api.dag.OutputPlanNode;
import tasks.CombineTask;
import tasks.ParallelDoMapTask;
import tasks.ParallelDoReduceTask;

import java.io.IOException;

/**
 * User: vjovanovic
 * Date: 4/23/11
 */
public class QuickTypeFixScalaI0 {
    static void setJobClassesBecause210SnapshotWillNot(Job job, boolean setCombiner, boolean setReducer, OutputPlanNode[] outputs) throws IOException {
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(ParallelDoMapTask.class);

        if (setReducer) {
            job.setReducerClass(ParallelDoReduceTask.class);
        }

        if (setCombiner) {
            job.setCombinerClass(CombineTask.class);
        }

        for (OutputPlanNode output : outputs) {
//            MultipleOutputs.addNamedOutput(job,
//                    output.collection().location().toString(), SequenceFileOutputFormat.class,
//                    NullWritable.class, BytesWritable.class);
        }
    }
}
