package scala.collection.distributed

import api._
import collection.immutable.{GenIterable, GenSeq}
import execution.DCUtil

trait RichDistProcessable[+T] extends DistProcessable[T] {
  def distDo[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]
  (distOp: (T, Emitter10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10], DistContext) => Unit):
  (DistIterable[T1], DistIterable[T2], DistIterable[T3], DistIterable[T4],
    DistIterable[T5], DistIterable[T6], DistIterable[T7], DistIterable[T8],
    DistIterable[T9], DistIterable[T10]) = {

    val collections = (1 to 10).map(v => CollectionId(DCUtil.generateNewCollectionURI))
    val rs = distDo(distOp, collections)

    (rs(0).asInstanceOf[DistIterable[T1]], rs(1).asInstanceOf[DistIterable[T2]], rs(2).asInstanceOf[DistIterable[T3]],
      rs(3).asInstanceOf[DistIterable[T4]], rs(4).asInstanceOf[DistIterable[T5]], rs(5).asInstanceOf[DistIterable[T6]],
      rs(6).asInstanceOf[DistIterable[T7]], rs(7).asInstanceOf[DistIterable[T8]], rs(8).asInstanceOf[DistIterable[T9]],
      rs(9).asInstanceOf[DistIterable[T10]])
  }

  def distDo[T](distOp: (T, Emitter[T], DistContext) => Unit): DistIterable[T] =
    distDo(distOp.asInstanceOf[(Any, IndexedEmitter, DistContext) => Unit],
      GenSeq(CollectionId(DCUtil.generateNewCollectionURI)))(0).asInstanceOf[DistIterable[T]]

  def distDo[T1, T2](distOp: (T, Emitter2[T1, T2], DistContext) => Unit): DistIterable[T] = {
    val collections = distDo(distOp, GenSeq(CollectionId(DCUtil.generateNewCollectionURI)))
    (rs(0).asInstanceOf[DistIterable[T1]], rs(1).asInstanceOf[DistIterable[T2]])
  }

  // TODO (VJ) 6 more methods for typed distDo

  // TODO (VJ) remove
  def parallelDo[B](parOperation: (T, Emitter[B], DistContext) => Unit): DistIterable[B] = distDo(parOperation)

  def groupBy[K, B](keyFunction: (T, Emitter[B]) => K): DistMap[K, GenIterable[B]] = sgbr(key = keyFunction)

  //TODO (VJ) remove
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
}