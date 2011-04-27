package scala.collection.distributed

import collection.{GenMapLike, MapLike}

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

trait DistMapLike[K, +V,
+Repr <: DistMapLike[K, V, Repr, Sequential] with DistMap[K, V], +Sequential <: Map[K, V] with MapLike[K, V, Sequential]]
  extends GenMapLike[K, V, Repr]
  with DistIterableLike[(K, V), Repr, Sequential] {
  self =>

  def default(key: K): V = throw new NoSuchElementException("key not found: " + key)

  def empty: Repr

  def apply(key: K) = get(key) match {
    case Some(v) => v
    case None => default(key)
  }

}