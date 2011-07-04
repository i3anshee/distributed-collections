package scala.colleciton.distributed.hadoop.shared

import collection.GenSeq
import collection.mutable
import mutable.ArrayBuffer
import collection.distributed.api.shared.{DSECollectionLike, CollectionType, DistSideEffects}
import collection.immutable

/**
 * @author Vojin Jovanovic
 */
class DSECollectionNode[T](val combineOp: Option[(Iterator[T] => T)] = None, val postProcessOp: Option[Iterator[T] => GenSeq[T]] = None, val collection: mutable.Buffer[T] = new ArrayBuffer[T]) extends DistSideEffects(CollectionType) with DSECollectionLike[T] {

  def combiner = combineOp

  def postProcess = postProcessOp

  def toMap[K, V](implicit ev: T <:< (K, V)): immutable.Map[K, V] = collection.toMap

  def seq = collection.toSeq

  def +=(value: T) = println(value)

}

object DSECollectionNode {
  def apply[T](data: Array[Byte]): DSECollectionNode[T] = throw new UnsupportedOperationException("Not implemented yet!!!")
}