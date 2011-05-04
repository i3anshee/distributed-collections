package scala.collection.distributed.api.dag

import collection.distributed.api.{ CollectionId}
import java.net.URI

case class InputPlanNode(uri: URI) extends PlanNode with CollectionId {
  def location = uri
}