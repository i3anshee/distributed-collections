package scala.collection.distributed.api

import dag.ExPlanDAG
import collection.mutable

/**
 * User: vjovanovic
 * Date: 4/1/11
 */

trait AbstractJobStrategy {
   def execute(dag: ExPlanDAG)
}