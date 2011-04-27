package scala.collection.distributed

import collection.{GenSet, GenSetLike, SetLike}

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

trait DistSetLike[T, +Repr <: DistSetLike[T, Repr, Sequential] with DistSet[T], +Sequential <: Set[T] with SetLike[T, Sequential]]
  extends GenSetLike[T, Repr]
  with DistIterableLike[T, Repr, Sequential] {
  self =>

  override def subsetOf(that: GenSet[T]): Boolean = throw new UnsupportedOperationException("Unsupported operation!!!")

  override def union(that: GenSet[T]): Repr = throw new UnsupportedOperationException("Unsupported operation!!!")

  override def intersect(that: GenSet[T]): Repr = throw new UnsupportedOperationException("Unsupported operation!!!")

  override def contains(elem: T): Boolean = throw new UnsupportedOperationException("Unsupported operation!!!")

  override def +(elem: T): Repr = throw new UnsupportedOperationException("Unsupported operation!!!")

  override def -(elem: T): Repr = throw new UnsupportedOperationException("Unsupported operation!!!")

}