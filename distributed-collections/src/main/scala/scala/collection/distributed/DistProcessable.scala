package scala.collection.distributed

import api.shared.DistBuilderLike
import collection.{GenTraversable, immutable}

/**
 * Base trait for collection operations based on shared nothing processing frameworks (Hadoop, Google MapReduce and Dryad).
 */
trait DistProcessable[+T] {

  // TODO add cache as the base operation (support for systems that have memory caching capabilities)
  protected def distForeach[U](distOp: T => U, distIterableBuilders: scala.Seq[DistBuilderLike[_, _]]): Unit

  protected def groupByKey[K, V](key: (T) => (K, V)): DistMap[K, immutable.GenIterable[V]]

  protected def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T]

  protected def combine[K, V, V1](combine: (Iterable[V]) => V1)(implicit ev: <:<[T, (K, V)]): DistIterable[(K, V1)]
}