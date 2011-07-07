package de.jungblut.clustering


import de.jungblut.clustering.model.Vector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import model.ClusterCenter
import org.apache.hadoop.io.{SequenceFile, IntWritable}
import scala.util.Random
import colleciton.distributed.hadoop.FSAdapter
import collection.mutable.ArrayBuffer
import org.apache.hadoop.mapreduce.Job
import io.{KryoRegistrator, KryoSerializer}

/**
 * @author Vojin Jovanovic
 */

object KMeansGenerate {
  def main(args: Array[String]) {
    var iteration: Int = 1
    System.setProperty("spark.kryo.registrator", "de.jungblut.clustering.collections.KryoRegistratorKMeans")
    val kryoSerializer = new KryoSerializer().newInstance

    val conf: Configuration = new Configuration
    conf.set("num.iteration", iteration + "")

    val in: Path = new Path("tmp/clustering/input")
    val inKryo = new Path("tmp/clustering/kryo-input")

    val center: Path = new Path("tmp/clustering/cen.seq")
    val centerKryo: Path = new Path("tmp/clustering/kryo-cen")


    conf.set("centroid.path", center.toString)


    val out: Path = new Path("tmp/clustering/depth_" + iteration)
    val job: Job = new Job(conf)
    job.setJobName("KMeans Generating")

    val fs: FileSystem = FileSystem.get(conf)


    if (fs.exists(out)) fs.delete(out, true)
    if (fs.exists(center)) fs.delete(center, true)
    if (fs.exists(centerKryo)) fs.delete(centerKryo, true)
    if (fs.exists(in)) fs.delete(in, true)
    if (fs.exists(inKryo)) fs.delete(inKryo, true)

    // write centers
    val centers: List[ClusterCenter] = List(
      new ClusterCenter(new Vector(150, 150)),
      new ClusterCenter(new Vector(600, 600)),
      new ClusterCenter(new Vector(450, 450)),
      new ClusterCenter(new Vector(300, 300)),
      new ClusterCenter(new Vector(750, 750))
    )

    val centerWriter: SequenceFile.Writer = SequenceFile.createWriter(fs, conf, center, classOf[ClusterCenter], classOf[IntWritable])
    val value: IntWritable = new IntWritable(0)
    centers.foreach(center => centerWriter.append(center, value))
    centerWriter.close
    val zippedCenters = centers.zipAll(List[Int](), centers.head, 0)
    FSAdapter.createDistCollection(centers, centerKryo.toUri, kryoSerializer)

    // write the data
    val dataWriter: SequenceFile.Writer = SequenceFile.createWriter(fs, conf, in, classOf[ClusterCenter], classOf[Vector])
    var buffer = new ArrayBuffer[(ClusterCenter, Vector)]
    (0 to 50000).foreach (v => {
      val center: ClusterCenter = new ClusterCenter(new Vector(100, 100))
      val vector: Vector = new Vector(Random.nextInt(1000), Random.nextInt(1000))

      buffer += ((center, vector));
      dataWriter.append(center, vector)
    })
    dataWriter.close

    FSAdapter.createDistCollection(buffer, inKryo.toUri, kryoSerializer)


  }

}