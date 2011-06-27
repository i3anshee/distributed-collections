package scala.collection.distributed.api.shared

import java.util.concurrent.atomic.AtomicLong
import collection.mutable
import collection.{GenSeq, immutable}
import java.io.{ObjectOutputStream, ByteArrayOutputStream}

/**
 * @author Vojin Jovanovic
 */

abstract class DistSideEffects(val varType: DSEType) extends Serializable {
  val id = DistSideEffects.newId

  override def equals(p1: Any) = p1.isInstanceOf[DistSideEffects] && id == p1.asInstanceOf[DistSideEffects].id

  override def hashCode = id.hashCode

  def computationData: Array[Byte]
}

object DistSideEffects {
  val idSeed = new AtomicLong(0)
  var sideEffects = new mutable.WeakHashMap[DistSideEffects with DSEProxy[_], Array[Byte]]

  def newId = idSeed.getAndIncrement

  def add(v: DistSideEffects with DSEProxy[_]) = sideEffects += (v -> v.computationData)

}

trait DSEProxy[T] {
  var impl: Option[T]
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

class DSECollectionProxy[T](@transient override var impl: Option[DSECollectionLike[T]] = None)
  extends DistSideEffects(CollectionType)
  with DSECollectionLike[T]
  with DSEProxy[DSECollectionLike[T]] {

  def combiner: Option[(Iterator[T] => T)] = impl.get.combiner

  def postProcess: Option[Iterator[T] => GenSeq[T]] = impl.get.postProcess

  def toMap[K, V](implicit ev: T <:< (K, V)): immutable.Map[K, V] = impl.get.toMap

  def seq = impl.get.seq

  def +=(value: T) = impl.get += value
}

trait DSEVar[T] {
  def +=(value: T)

  // add implicits for getting the var value
}

trait DSECounter {
  def increment
}


sealed abstract class DSEType

case object CollectionType extends DSEType

case object VarType extends DSEType

case object CounterType extends DSEType
