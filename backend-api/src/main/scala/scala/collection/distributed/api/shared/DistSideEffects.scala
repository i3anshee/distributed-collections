package scala.collection.distributed.api.shared

import java.util.concurrent.atomic.AtomicLong
import collection.mutable
import collection.{GenSeq, immutable}
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.net.URI
import collection.distributed.api.ReifiedDistCollection

/**
 * @author Vojin Jovanovic
 */

abstract class DistSideEffects(val varType: DSEType) extends Serializable {
  protected val id = DistSideEffects.newId

  def uid: Long = id

  def computationData: Array[Byte]

  override def equals(p1: Any) = p1.isInstanceOf[DistSideEffects] && uid == p1.asInstanceOf[DistSideEffects].uid

  override def hashCode = uid.hashCode
}

object DistSideEffects {
  val idSeed = new AtomicLong(0)

  var sideEffectsData: mutable.Map[DistSideEffects with DSEProxy[_ <: DistSideEffects], Array[Byte]] = new mutable.WeakHashMap

  def newId = idSeed.getAndIncrement

  def add(v: DistSideEffects with DSEProxy[_ <: DistSideEffects]) = sideEffectsData += (v -> v.computationData)

  def findImpl(id: Long): DistSideEffects = sideEffectsData.keys.find(_.uid == id).get.impl

}

trait DSEProxy[T] {
  @transient var impl: T
}

trait DSECounterLike {
  def +=(n: Long)

  def apply(): Long

  def computationData = {
    val buffer = ByteBuffer.allocate(8)
    buffer.putLong(apply())
    buffer.array
  }
}


class DSECounterProxy(proxyImpl: DistSideEffects with DSECounterLike)
  extends DistSideEffects(CounterType) with DSECounterLike with DSEProxy[DistSideEffects with DSECounterLike] {
  override val id = proxyImpl.uid
  @transient override var impl: DistSideEffects with DSECounterLike = proxyImpl

  private def findImpl: DistSideEffects with DSECounterLike = {
    if (impl == null)
      impl = DistSideEffects.findImpl(id).asInstanceOf[DistSideEffects with DSECounterLike]
    impl
  }

  def +=(n: Long) = findImpl += n

  def apply() = findImpl.apply()
}


trait DistIterableBuilderLike[T, Repr] extends ReifiedDistCollection {

  def +=(element: T)

  def result(): Repr

  def computationData = location.toString.getBytes()
}

class DistIterableBuilderProxy[T, Repr](@transient override var impl: DistSideEffects with DistIterableBuilderLike[T, Repr]  = null)
  extends DistSideEffects(CollectionType)
  with DistIterableBuilderLike[T, Repr]
  with DSEProxy[DistSideEffects with DistIterableBuilderLike[T, Repr]] {

  private def findImpl: DistIterableBuilderLike[T, Repr] = {
    if (impl == null)
      impl = DistSideEffects.findImpl(id).asInstanceOf[DistSideEffects with DistIterableBuilderLike[T, Repr]]
    impl
  }

  override def equals(p1: Any) = DistSideEffects.equals(p1)

  override def hashCode = DistSideEffects.hashCode

  def location = findImpl.location

  def result() = findImpl.result

  def +=(value: T) = findImpl += value

}

sealed abstract class DSEType

case object CollectionType extends DSEType

case object CounterType extends DSEType

