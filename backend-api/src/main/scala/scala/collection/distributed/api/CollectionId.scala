package scala.collection.distributed.api

import java.net.URI
import java.io.Serializable
import java.util.concurrent.atomic.AtomicLong


trait UniqueId extends Serializable {

  protected[this] val uniqueId: Long

  def id: Long = uniqueId

  override def equals(p1: Any) = (p1.isInstanceOf[UniqueId] && p1.asInstanceOf[UniqueId].id == id)

  override def hashCode = id.hashCode()

}

object UniqueId {

  private[this] val counter: AtomicLong = new AtomicLong(0L)

  def apply(): Long = counter.incrementAndGet

}


trait CollectionId extends Serializable {
  def location: URI

  override def equals(p1: Any) = (p1.isInstanceOf[CollectionId] && p1.asInstanceOf[CollectionId].location == location)

  override def hashCode = location.hashCode
}

object CollectionId {
  def apply(uri: URI): CollectionId = new CollectionId {
    def location = uri
  }
}
