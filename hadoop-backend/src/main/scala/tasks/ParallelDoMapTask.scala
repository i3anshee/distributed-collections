package tasks

import org.apache.hadoop.mapreduce.Mapper
import scala.collection.distributed.api.{DistContext}
import org.apache.hadoop.io.{NullWritable, BytesWritable}
import collection.distributed.api.dag.ExPlanDAG

class ParallelDoMapTask extends DistributedCollectionsMapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  // closure to be invoked
  var mapDAG: ExPlanDAG = null


  override def setup(context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context) {
    super.setup(context)

    val conf = context.getConfiguration
    mapDAG = deserializeOperation(conf, "distcoll.mapperDAG").get

    // every output with its
  }

  override def distMap(k: NullWritable, v: BytesWritable, context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context, distContext: DistContext): Unit = {

    val value = deserializeElement(v.getBytes())



  }
}

