package scala.collection.distributed.api.shared

import java.util.concurrent.atomic.AtomicLong
import java.nio.ByteBuffer
import java.net.URI
import collection.mutable.Builder
import collection.mutable
import collection.distributed.api.{RecordNumber, ReifiedDistCollection}

/**
 * @author Vojin Jovanovic
 */

abstract class DistSideEffects(val varType: DSEType, newId: Long) extends Serializable {
  def this(varType: DSEType) = this (varType, DistSideEffects.newId)

  protected val id = newId

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

  def findImpl(id: Long): DistSideEffects = {
    sideEffectsData.keys.find(_.uid == id).get.impl
  }
}

trait DSEProxy[T] {
  @transient var impl: T
}

sealed abstract class DSEType

case object CollectionType extends DSEType

case object CounterType extends DSEType

case object RecordType extends DSEType

trait DistCounterLike {
  def +=(n: Long)

  def apply(): Long

  def computationData = {
    val buffer = ByteBuffer.allocate(8)
    buffer.putLong(apply())
    buffer.array
  }
}

trait DistBuilderLike[-Elem, +To] extends Builder[Elem, To] with ReifiedDistCollection {

  def uniqueElementsBuilder: DistSideEffects with DistBuilderLike[Elem, To]

  def result(uri: URI): To

  def result(): To

  def clear() {
    throw new UnsupportedOperationException("Remote builders can not be cleared as the element addition is part of the framework!!!")
  }

  def applyConstraints()

  def computationData = location.toString.getBytes()

  def uniqueElements: Boolean
}

trait RecordCounterLike {
  def fileNumber: Int

  def recordStart: Long

  def counter: Long

  def recordNumber = RecordNumber(fileNumber, recordStart, counter)

  def computationData = Array[Byte](0)
}

class DistRecordCounterProxy(proxyImpl: DistSideEffects with RecordCounterLike)
  extends DistSideEffects(RecordType) with RecordCounterLike with DSEProxy[DistSideEffects with RecordCounterLike] {
  override val id = proxyImpl.uid
  @transient override var impl: DistSideEffects with RecordCounterLike = proxyImpl

  private def findImpl: DistSideEffects with RecordCounterLike = {
    if (impl == null)
      impl = DistSideEffects.findImpl(id).asInstanceOf[DistSideEffects with RecordCounterLike]
    impl
  }

  def counter = findImpl.counter

  def recordStart = findImpl.recordStart

  def fileNumber = findImpl.fileNumber
}

class DistCounterProxy(proxyImpl: DistSideEffects with DistCounterLike)
  extends DistSideEffects(CounterType) with DistCounterLike with DSEProxy[DistSideEffects with DistCounterLike] {
  override val id = proxyImpl.uid
  @transient override var impl: DistSideEffects with DistCounterLike = proxyImpl

  private def findImpl: DistSideEffects with DistCounterLike = {
    if (impl == null)
      impl = DistSideEffects.findImpl(id).asInstanceOf[DistSideEffects with DistCounterLike]
    impl
  }

  def +=(n: Long) {
    findImpl += n
  }

  def apply() = findImpl.apply()
}

class DistBuilderProxy[T, Repr](@transient override var impl: DistSideEffects with DistBuilderLike[T, Repr] = null)
  extends DistSideEffects(CollectionType, impl.uid)
  with DistBuilderLike[T, Repr]
  with DSEProxy[DistSideEffects with DistBuilderLike[T, Repr]] {

  private def findImpl: DistBuilderLike[T, Repr] = {
    if (impl == null)
      impl = DistSideEffects.findImpl(id).asInstanceOf[DistSideEffects with DistBuilderLike[T, Repr]]
    impl
  }

  override def equals(p1: Any) = DistSideEffects.equals(p1)

  override def hashCode = DistSideEffects.hashCode

  def location = findImpl.location

  def result() = findImpl.result()

  def result(uri: URI) = findImpl.result(uri)

  def +=(value: T) = {
    findImpl += value
    this
  }

  def uniqueElements = findImpl.uniqueElements

  def uniqueElementsBuilder = {
    val proxy = new DistBuilderProxy[T, Repr](impl.uniqueElementsBuilder)
    DistSideEffects.add(proxy)
    proxy
  }

  def applyConstraints() {
    findImpl.applyConstraints()
  }
}
