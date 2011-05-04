package scala.collection.distributed.api

import java.net.URI
import java.io.Serializable
import java.util.concurrent.atomic.AtomicLong


trait CollectionId extends Serializable {
  def location: URI
}

trait UniqueId extends Serializable {

  val idValue = UniqueId()

  def id: Long = idValue

}

trait DistLocation extends Serializable {
  def location: URI
}

object CollectionId {
  def apply(uri: URI): CollectionId = new CollectionId {
    def location = uri
  }
}

object UniqueId {
  private val counter: AtomicLong = new AtomicLong(0L)

  def apply(): Long = counter.incrementAndGet
}