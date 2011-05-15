package scala.collection.distributed

import collection.GenSet
import collection.generic.GenericCompanion

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

trait DistSet[T] extends GenSet[T]
with GenericDistTemplate[T, DistSet]
with DistIterable[T]
with DistSetLike[T, DistSet[T], Set[T]] {
  self =>
  override def empty: DistSet[T] = throw new UnsupportedOperationException("")

  //protected[this] override def newCombiner: Combiner[T, ParSet[T]] = ParSet.newCombiner[T]

  override def companion: GenericCompanion[DistSet] with GenericDistCompanion[DistSet] = DistSet

  override def seq = super.seq.toSet

  override def stringPrefix = "DistSet"

//  def contains(elem: A): Boolean = exists((p) => p == elem)
//
//  def +(elem: A): DistSetOld[A] = {
//    var alreadyAdded = false
//    ensureSet(parallelDo((el: A, em: Emitter[A], context: DistContext) => {
//      if (!alreadyAdded) {
//        em.emit(elem)
//        alreadyAdded = true
//      }
//      em.emit(el)
//    }))
//  }
//
//  def -(elem: A): DistSetOld[A] = new DistSetOld[A](filter(p => p != elem).location)
//
//  def apply(elem: A): Boolean = contains(elem)
//
//  def intersect(that: DistSetOld[A]): DistSetOld[A] = null
//
//  def &(that: DistSetOld[A]): DistSetOld[A] = intersect(that)
//
//  def union(that: DistSetOld[A]): DistSetOld[A] = ensureSet(this.++(that))
//
//  def |(that: DistSetOld[A]): DistSetOld[A] = union(that)
//
//  def diff(that: DistSetOld[A]): DistSetOld[A] = --(that)
//
//  def --(that: DistCollection[A]): DistSetOld[A] = new DistSetOld[A](
//    parallelDo((el, em: Emitter[(A, Boolean)]) => em.emit((el, true))).
//      flatten(List(
//      that.parallelDo((el, em: Emitter[(A, Boolean)]) => em.emit((el, false))))).
//      groupBy((el: (A, Boolean), em: Emitter[Boolean]) => {
//      em.emit(el._2)
//      el._1
//    }).distDo((el: (A, Iterable[Boolean]), em: Emitter[A]) => if (el._2.size == 1 && el._2.head == true) em.emit(el._1)).location)
//
//  def &~(that: DistSetOld[A]): DistSetOld[A] = diff(that)
}

object DistSet extends DistSetFactory[DistSet] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistSet[T]] = new GenericDistBuildFrom[T]

  def newRemoteBuilder[T]: RemoteBuilder[T, DistSet[T]] = new DistSetRemoteBuilder[T]
}
