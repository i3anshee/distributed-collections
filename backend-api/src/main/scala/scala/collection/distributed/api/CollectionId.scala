package scala.collection.distributed.api

import java.net.URI
import java.io.Serializable

/**
 * User: vjovanovic
 * Date: 4/5/11
 */

trait CollectionId extends Serializable {
  def location:URI
}

object CollectionId {
  def apply(uri: URI): CollectionId = new CollectionId{
    def location = uri
  }
}