package execution

import scala.collection.distributed.api.dag.ExPlanDAG
import scala.collection.distributed.api.AbstractJobStrategy
import scala.colleciton.distributed.hadoop.HadoopJob

/**
 * User: vjovanovic
 * Date: 4/1/11
 */

object JobExecutor {
  val myStrategy: AbstractJobStrategy = HadoopJob

  def execute(dag: ExPlanDAG) {
    myStrategy.execute(dag)
  }
}