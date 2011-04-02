package dcollections.api

import dag.ExPlanDAG

/**
 * User: vjovanovic
 * Date: 4/1/11
 */

trait AbstractJobStrategy {
   def execute(dag: ExPlanDAG)
}