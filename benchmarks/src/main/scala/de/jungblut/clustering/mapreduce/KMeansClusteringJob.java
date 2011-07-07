/*
 * Code for k-means clustering is taken from Thomas Jungblut at http://code.google.com/p/hama-shortest-paths.
 * Thanks Thomas.
 */
package de.jungblut.clustering.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import de.jungblut.clustering.model.ClusterCenter;
import de.jungblut.clustering.model.Vector;

public class KMeansClusteringJob {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        int iteration = 1;
        Configuration conf = new Configuration();
        conf.set("num.iteration", iteration + "");

        Path in = new Path("tmp/clustering/input");
        Path center = new Path("tmp/clustering/cen.seq");
        conf.set("centroid.path", center.toString());
        Path out = new Path("tmp/clustering/depth_" + iteration);

        Job job = new Job(conf);
        job.setJobName("KMeans Clustering");

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setJarByClass(KMeansMapper.class);

        SequenceFileInputFormat.addInputPath(job, in);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out))
            fs.delete(out, true);

        SequenceFileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(ClusterCenter.class);
        job.setOutputValueClass(Vector.class);

        job.waitForCompletion(true);

        long counter = job.getCounters()
                .findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        iteration++;
        while (counter > 0) {
            conf = new Configuration();
            conf.set("centroid.path", center.toString());
            conf.set("num.iteration", iteration + "");
            job = new Job(conf);
            job.setJobName("KMeans Clustering " + iteration);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setJarByClass(KMeansMapper.class);

            in = new Path("tmp/clustering/depth_" + (iteration - 1) + "/");
            out = new Path("tmp/clustering/depth_" + iteration);

            SequenceFileInputFormat.addInputPath(job, in);

            if (fs.exists(out))
                fs.delete(out, true);

            SequenceFileOutputFormat.setOutputPath(job, out);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(ClusterCenter.class);
            job.setOutputValueClass(Vector.class);

            job.waitForCompletion(true);
            iteration++;
            counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        }

        Path result = new Path("tmp/clustering/depth_" + (iteration - 1));

        FileStatus[] statuses = fs.listStatus(result);
        for (FileStatus status : statuses) {
            Path path = status.getPath();
            if (path.toString().contains("SUCCESS")) continue;
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
            ClusterCenter key = new ClusterCenter();
            Vector v = new Vector();
            while (reader.next(key, v)) {
                System.out.println(key + " / " + v);
            }
            reader.close();
        }
    }
}