package mrapi

import org.apache.hadoop.mapreduce.Job


/**
 * User: vjovanovic
 * Date: 3/21/11
 */

trait TaskAdapter {
  def prepare(job: Job): Unit
}

