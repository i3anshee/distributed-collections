package scala.collection.distributed.shared

import collection.{immutable, mutable}
import immutable.GenSeq
import mutable.ArrayBuffer
import collection.distributed.api.shared.{DSECollectionProxy, CollectionType, DistSideEffects, DSECollectionLike}

/**
 * @author Vojin Jovanovic
 */
class DSECollection[T](val combineOp: Option[(Iterator[T] => T)] = None, val postProcessOp: Option[Iterator[T] => GenSeq[T]] = None, val collection: mutable.Buffer[T] = new ArrayBuffer[T]) extends DistSideEffects(CollectionType) with DSECollectionLike[T] {

  def combiner = combineOp

  def postProcess = postProcessOp

  def toMap[K, V](implicit ev: T <:< (K, V)): immutable.Map[K, V] = collection.toMap

  def seq = collection.toSeq

  def +=(value: T) = collection += value

}

object DSECollection {

  def apply[T](combiner: Option[(Iterator[T] => T)] = None,
               postProcess: Option[Iterator[T] => GenSeq[T]] = None,
               collection: mutable.Buffer[T] = new ArrayBuffer[T]): DSECollectionLike[T] = {
    val proxy = new DSECollectionProxy[T](new DSECollection[T](combiner, postProcess, collection))
//    DistSideEffects.add(proxy)
    proxy
  }

}