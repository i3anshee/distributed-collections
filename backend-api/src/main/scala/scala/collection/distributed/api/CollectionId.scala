package scala.collection.distributed.api

import java.net.URI
import java.io.Serializable
import java.util.concurrent.atomic.AtomicLong


trait DistLocation extends Serializable {
  def location: URI
}

trait UniqueId extends Serializable {

  protected[this] val uniqueId: Long

  def id: Long = uniqueId

}

object UniqueId {

  private val counter: AtomicLong = new AtomicLong(0L)

  def apply(): Long = counter.incrementAndGet

}

trait CollectionId extends Serializable {
  def location: URI
}

object CollectionId {

  def apply(uri: URI): CollectionId = new CollectionId {
    def location = uri
  }

}
