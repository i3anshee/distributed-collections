package mrapi

import org.apache.hadoop.mapreduce.Job


/**
 * User: vjovanovic
 * Date: 3/21/11
 */

abstract class ReducerAdapter extends TaskAdapter {
  def prepare(job: Job): Unit
}
