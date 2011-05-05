package scala.collection.distributed.api.dag

import java.net.URI
import collection.distributed.api.{UniqueId, CollectionId}
import collection.mutable

case class OutputPlanNode(uri: URI,
                          inEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                          outEdges: mutable.Map[PlanNode, CollectionId] = new mutable.HashMap[PlanNode, CollectionId],
                          uniqueId: Long = UniqueId()) extends PlanNode with CollectionId {
  def location = uri
  def copyUnconnected() = copy(inEdges = mutable.HashMap(), outEdges = mutable.HashMap())
}