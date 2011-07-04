package scala.collection.distributed.api.shared

import java.util.concurrent.atomic.AtomicLong
import collection.mutable
import collection.{GenSeq, immutable}
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

/**
 * @author Vojin Jovanovic
 */

abstract class DistSideEffects(val varType: DSEType) extends Serializable {
  protected val id = DistSideEffects.newId

  def uid: Long = id

  def computationData: Array[Byte]

  final override def equals(p1: Any) = p1.isInstanceOf[DistSideEffects] && uid == p1.asInstanceOf[DistSideEffects].uid

  final override def hashCode = uid.hashCode
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


trait DSECollectionLike[T] {

  def combiner: Option[(Iterator[T] => T)]

  def postProcess: Option[Iterator[T] => GenSeq[T]]

  def +=(value: T)

  def seq: GenSeq[T]

  def toMap[K, V](implicit ev: T <:< (K, V)): immutable.Map[K, V]

  def computationData = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject((combiner, postProcess))
    out.close()
    bos.toByteArray
  }

}

class DSECollectionProxy[T](@transient override var impl: DSECollectionLike[T] = null)
  extends DistSideEffects(CollectionType)
  with DSECollectionLike[T]
  with DSEProxy[DSECollectionLike[T]] {

  private def findImpl: DSECollectionLike[T] = {
    if (impl == null)
      impl = DistSideEffects.findImpl(id).asInstanceOf[DSECollectionLike[T]]
    impl
  }

  def combiner: Option[(Iterator[T] => T)] = findImpl.combiner

  def postProcess: Option[Iterator[T] => GenSeq[T]] = findImpl.postProcess

  def toMap[K, V](implicit ev: T <:< (K, V)): immutable.Map[K, V] = findImpl.toMap

  def seq = findImpl.seq

  def +=(value: T) = findImpl += value

}

trait DSEVar[T] {
  def +=(value: T)
}

sealed abstract class DSEType

case object CollectionType extends DSEType

case object VarType extends DSEType

case object CounterType extends DSEType

