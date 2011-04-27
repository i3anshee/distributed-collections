package scala.collection.distributed

import api.{DistContext, Emitter}
import collection.immutable.{GenMap, GenIterable, GenTraversable}

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

trait DistProcessable[+T, Repr] {

  def parallelDo[B](parOperation: (T, Emitter[B], DistContext) => Unit): DistIterable[B]

  def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T]

  def groupBy[K, B](keyFunction: (T, Emitter[B]) => K): DistMap[K, GenIterable[B]]

  def combineValues[K, B, C](keyFunction: (T, Emitter[B]) => K, op: (C, B) => C): DistMap[K, C]

  def sort[B](by: (T) => Ordered[B]): DistIterable[T]

  def parallelDo[B](parOperation: (T, Emitter[B]) => Unit): DistIterable[B] =
    parallelDo((el: T, em: Emitter[B], context: DistContext) => parOperation(el, em))

  def flatten[B >: T](distIterable1: DistIterable[B]): DistIterable[T] = flatten(List(distIterable1))

  def flatten[B >: T, C >: T](distIterable1: DistIterable[B], distIterable2: DistIterable[C]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2))

  def flatten[B >: T, C >: T, D >: T](distIterable1: DistIterable[B], distIterable2: DistIterable[C],
                                      distIterable3: DistIterable[D]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2, distIterable3))

  def flatten[B >: T, C >: T, D >: T, E >: T](distIterable1: DistIterable[B], distIterable2: DistIterable[C],
                                              distIterable3: DistIterable[D], distIterable4: DistIterable[E]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2, distIterable3, distIterable4))

  // Possible combinations of operations
  //  {[do | flatten]}
  //  groupBySortCombine
  //  groupBySort
  //  groupByCombine
  //  groupBy
  //  Sort
  //  {do}

}