package scala.colleciton.distributed.hadoop.shared

import java.net.URI
import collection.distributed.api.shared.{CollectionType, DistIterableBuilderLike, DistSideEffects}
import tasks.dag.RuntimePlanNode
import collection.distributed.api.{DistContext}

/**
 * @author Vojin Jovanovic
 */

class DistIterableBuilderNode(val uri: URI)
  extends DistSideEffects(CollectionType) with DistIterableBuilderLike[Any, Any] {

  var output: (RuntimePlanNode, Traversable[RuntimePlanNode]) = null

  def result() = throw new RuntimeException("Creation of ditributed collecitons is impossible in cluster nodes!s");

  def +=(element: Any) = {
    output._2.foreach(v =>{
      v.execute(output._1, new DistContext(), null, element)
    })
  }

  def location = uri
}