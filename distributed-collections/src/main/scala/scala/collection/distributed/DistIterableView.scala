package scala.collection.distributed

import collection.{IterableView, GenIterableView}
import execution.ExecutionPlan

/**
 * @author Vojin Jovanovic
 */

trait DistIterableView[+T, +Coll, +CollSeq]
//extends DistIterableViewLike[T, Coll, CollSeq, DistIterableView[T, Coll, CollSeq], IterableView[T, CollSeq]]
//with GenIterableView[T, Coll] {
//  def isView = true
//}
//
//
//object DistIterableView {
//  type Coll = DistIterableView[_, C, _] forSome {type C <: DistIterable[_]}
//
//  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistIterableView[T, DistIterable[T], Iterable[T]]] = null
//
//}