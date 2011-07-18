package scala.colleciton.distributed.hadoop.shared

import java.net.URI
import collection.distributed.api.shared.{CollectionType, DistBuilderLike, DistSideEffects}
import tasks.dag.RuntimePlanNode
import collection.distributed.api.{DistContext}

/**
 * @author Vojin Jovanovic
 */

class DistBuilderNode(val uri: URI)
  extends DistSideEffects(CollectionType) with DistBuilderLike[Any, Any] {

  var output: (RuntimePlanNode, Traversable[RuntimePlanNode]) = null

  def result() = throw new RuntimeException("Creation of ditributed collecitons is impossible in cluster nodes!s");
  def result(uri : URI) = throw new RuntimeException("Creation of ditributed collecitons is impossible in cluster nodes!s");

  def +=(element: Any) = {
    output._2.foreach(v => {
      v.execute(output._1, new DistContext(), null, element)
    })

    this
  }


  def applyConstraints = throw new UnsupportedOperationException("Constraints can be applied only on client side!!!!")

  def location = uri

  def uniqueElements = throw new UnsupportedOperationException("Client side does not have access to builder elements information!!!")

  def uniqueElementsBuilder = throw new UnsupportedOperationException("Client side can not create a new builder!!!")
}