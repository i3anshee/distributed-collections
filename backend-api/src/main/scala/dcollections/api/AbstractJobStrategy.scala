package dcollections.api

import dag.ExPlanDAG
import collection.mutable

/**
 * User: vjovanovic
 * Date: 4/1/11
 */

trait AbstractJobStrategy {
   def execute(dag: ExPlanDAG, globalCache: mutable.Map[String, Any])
}