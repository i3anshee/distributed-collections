package scala.collection.distributed

import api._
import java.net.URI
import collection.immutable.{GenSeq, GenIterable, GenTraversable}


/**
 * Base trait for collection operations based on shared nothing processing frameworks (Hadoop, Google MapReduce and Dryad).
 */
trait DistProcessable[+T] {

  def distDo(distOp: (T, UntypedEmitter, DistContext) => Unit, outputs: GenSeq[(CollectionId, Manifest[_])]): GenSeq[DistIterable[Any]]

  def groupBySort[S, K, K1 <: K,  T1](key: (T, Emitter[T1]) => K, by: (K1) => Ordered[S] = nullOrdered[K]): DistMap[K, GenIterable[T1]] with DistCombinable[K, T1]

  def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T]

  protected[this] val NullOrdered = (el: T) => null

  protected[this] def nullOrdered[K] = NullOrdered.asInstanceOf[(K) => Ordered[K]]

}

trait DistCombinable[K, +T] {
  def combine[T1 >: T](combine: (Iterable[T]) => T1): DistMap[K, T1]
}


