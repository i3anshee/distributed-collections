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
    newRemoteBuilder.result(distDo((el: T, em: Emitter[T]) => {
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


  def --(that: GenTraversableOnce[T]): Repr = newRemoteBuilder.result(
    distDo((el, em: Emitter[(T, Boolean)]) => em.emit((el, true))).
      flatten(List(
      that.asInstanceOf[DistIterable[T]].distDo((el, em: Emitter[(T, Boolean)]) => em.emit((el, false))))).
      groupBySort((el: (T, Boolean), em: Emitter[Boolean]) => {
      em.emit(el._2)
      el._1
    }).distDo((el: (T, GenIterable[Boolean]), em: Emitter[T]) => if (el._2.size == 1 && el._2.head == true) em.emit(el._1)))

  def union(that: GenSet[T]): Repr = {
    val builder = newRemoteBuilder
    builder.uniquenessPreserved
    builder.result(this.++(that))
  }

  def diff(that: GenSet[T]): Repr = --(that)

  override def subsetOf(that: GenSet[T]): Boolean = throw new UnsupportedOperationException("Unsupported operation!!!")

  override def intersect(that: GenSet[T]): Repr = throw new UnsupportedOperationException("Unsupported operation!!!")

}