package mrapi

import org.apache.hadoop.mapreduce.Job
import tasks.ToOneKeyMap

/**
 * User: vjovanovic
 * Date: 3/23/11
 */

class ToOneKeyMapperAdapter extends MapperAdapter {
  def prepare(job: Job) = {
    job.setMapperClass(classOf[ToOneKeyMap])
  }
}