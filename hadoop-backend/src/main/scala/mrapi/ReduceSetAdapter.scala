package mrapi

import org.apache.hadoop.mapreduce.Job
import tasks.ReduceSet

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

class ReduceSetAdapter extends ReducerAdapter {
  def prepare(job: Job) = job.setReducerClass(classOf[ReduceSet])
}