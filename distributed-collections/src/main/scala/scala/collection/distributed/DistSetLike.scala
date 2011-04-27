package scala.collection.distributed

import api.Emitter
import collection.{GenSet, GenSetLike, SetLike}

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

trait DistSetLike[T, +Repr <: DistSetLike[T, Repr, Sequential] with DistSet[T], +Sequential <: Set[T] with SetLike[T, Sequential]]
  extends GenSetLike[T, Repr]
  with DistIterableLike[T, Repr, Sequential] {
  self =>

  def union(that: GenSet[T]): Repr = this.++(that)

  def contains(elem: T): Boolean = exists(_ == elem)

  def +(elem: T): Repr = {
    var alreadyAdded = false
    newRemoteBuilder.result(parallelDo((el: T, em: Emitter[T]) => {
      if (!alreadyAdded) {
        em.emit(elem)
        alreadyAdded = true
      }
      em.emit(el)
    }))
  }

  def -(elem: T): Repr = {
    val rb = newRemoteBuilder
    rb.uniquenessPreserved
    rb.result(filter(p => p != elem))
  }

  // TODO (VJ) waiting for new flatten
  def diff(that: GenSet[T]) = throw new UnsupportedOperationException("Unsupported operation!!!")

  override  def subsetOf(that: GenSet[T]): Boolean = throw new UnsupportedOperationException("Unsupported operation!!!")

  override def intersect(that: GenSet[T]): Repr = throw new UnsupportedOperationException("Unsupported operation!!!")

}