package scala.colleciton.distributed.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import scala.collection.distributed.api.dag.OutputPlanNode;
import tasks.DistributedCollectionsCombine;
import tasks.DistributedCollectionsMap;
import tasks.DistributedCollectionsReduce;

import java.io.IOException;

/**
 * User: vjovanovic
 * Date: 4/23/11
 */
public class QuickTypeFixScalaI0 {
    static void setJobClassesBecause210SnapshotWillNot(JobConf job, boolean setCombiner, boolean setReducer, OutputPlanNode[] outputs) throws IOException {
        job.setInputFormat(SequenceFileInputFormat.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);
        job.setMapperClass(DistributedCollectionsMap.class);

        if (setReducer) {
            job.setReducerClass(DistributedCollectionsReduce.class);
        }

        if (setCombiner) {
            job.setCombinerClass(DistributedCollectionsCombine.class);
        }

        for (OutputPlanNode output : outputs) {
            MultipleOutputs.addNamedOutput(job,
                    output.collection().location().toString(), SequenceFileOutputFormat.class,
                    NullWritable.class, BytesWritable.class);
        }
    }
}
