package scala.colleciton.distributed.hadoop

import collection.distributed.api.dag.ExPlanDAG
import org.apache.hadoop.mapreduce.{Job}

trait MSCRBuilder {

  def build(dag: ExPlanDAG): ExPlanDAG

  def configure(job: Job)

}