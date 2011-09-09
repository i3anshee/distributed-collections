package scala.collection.distributed.hadoop

import collection.distributed.api.dag.{OutputPlanNode, ExPlanDAG}
import collection.distributed.api.ReifiedDistCollection
import org.apache.hadoop.mapred.{RunningJob, JobConf}

trait JobBuilder {

  def build(dag: ExPlanDAG): ExPlanDAG

  def configure(job: JobConf)

  def outputs : Traversable[ReifiedDistCollection]

  def postRun(conf: JobConf, runningJob: RunningJob)
}