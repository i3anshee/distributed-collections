package scala.collection.distributed

import api.CollectionId
import java.net.URI

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

class DistHashSet[T](uri: URI) extends DistSet[T]
with GenericDistTemplate[T, DistHashSet]
with CollectionId {
  def location = uri
}