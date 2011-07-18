package scala.collection.distributed

import api.Emitter
import collection.{GenTraversableOnce, GenSet, GenSetLike, SetLike}
import collection.immutable.GenIterable

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

trait DistSetLike[T, +Repr <: DistSetLike[T, Repr, Sequential] with DistSet[T], +Sequential <: Set[T] with SetLike[T, Sequential]]
  extends GenSetLike[T, Repr]
  with DistIterableLike[T, Repr, Sequential] {
  self =>

  def contains(elem: T): Boolean = exists(_ == elem)

  def +(elem: T): Repr = {
    var alreadyAdded = false
    val rb = newDistBuilder
    foreach(el => {
      if (!alreadyAdded) {
        rb += elem
        alreadyAdded = true
      }
      rb += el
    })
    rb.result
  }

  def -(elem: T): Repr = filter(p => p != elem)

  //TODO (VJ) fix the constraints for sets (apparently views should not enforce constraints until forced)
  //TODO (VJ) compare performance to native
  def --(that: GenTraversableOnce[T]): Repr = throw new UnsupportedOperationException("Not implemented yet!!!")
//    map(v => (v, true)) ++
//      that.asInstanceOf[DistIterable[T]].map(v => (v, false))
//        .groupByKey
//        .filter(el => el._2.size == 1 && el._2.head == true).map(_._1)

  def union(that: GenSet[T]): Repr = throw new UnsupportedOperationException("Not implemented yet!!!")

  def diff(that: GenSet[T]): Repr = this -- that

  override def subsetOf(that: GenSet[T]): Boolean = throw new UnsupportedOperationException("Unsupported operation!!!")

  override def intersect(that: GenSet[T]): Repr = throw new UnsupportedOperationException("Unsupported operation!!!")

}