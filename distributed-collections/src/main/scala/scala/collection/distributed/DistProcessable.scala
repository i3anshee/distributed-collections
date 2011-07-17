package scala.collection.distributed

import api._
import api.shared.DistBuilderLike
import collection.{GenTraversable, immutable}

/**
 * Base trait for collection operations based on shared nothing processing frameworks (Hadoop, Google MapReduce and Dryad).
 */
trait DistProcessable[+T] {

  // TODO add cache as the base operation (support for systems that have memory caching capabilities)
  def distDo(distOp: (T, UntypedEmitter, DistContext) => Unit, outputs: immutable.GenSeq[(CollectionId, Manifest[_])]): immutable.GenSeq[DistIterable[Any]]

  protected[this] def groupBySort[S, K, K1 <: K, T1](key: (T, Emitter[T1]) => K, by: (K1) => Ordered[S] = nullOrdered[K]): DistMap[K, immutable.GenIterable[T1]] with DistCombinable[K, T1]

  protected[this] def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T]

  protected[this] def distForeach[U](distOp: T => U,
                                     distIterableBuilders: scala.Seq[DistBuilderLike[_, _]]): Unit

  protected[this] val NullOrdered = (el: T) => null

  protected[this] def nullOrdered[K] = NullOrdered.asInstanceOf[(K) => Ordered[K]]

}

trait DistCombinable[K, +T] {
  def combine[T1 >: T](combine: (Iterable[T]) => T1): DistMap[K, T1]
}