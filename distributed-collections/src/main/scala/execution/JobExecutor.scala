package execution

import scala.collection.distributed.api.dag.ExPlanDAG
import scala.collection.distributed.api.AbstractJobStrategy
import collection.mutable
import scala.colleciton.distributed.hadoop.HadoopJob

/**
 * User: vjovanovic
 * Date: 4/1/11
 */

object JobExecutor {
  val myStrategy: AbstractJobStrategy = HadoopJob

  def execute(dag: ExPlanDAG, globalCache: mutable.Map[String, Any]) {
    myStrategy.execute(dag, globalCache)
  }
}