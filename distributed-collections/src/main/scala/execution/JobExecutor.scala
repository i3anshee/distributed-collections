package execution

import dcollections.api.dag.ExPlanDAG
import dcollections.api.AbstractJobStrategy
import collection.mutable
import mrapi.HadoopJob

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