package scala.collection.distributed

import api._
import execution.DCUtil

// TODO (VJ) 6 more methods for typed distDo
// TODO (VJ) add more flattens
trait RichDistProcessable[+T] extends DistProcessable[T] {

  def distDo[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]
  (distOp: (T, Emitter10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10], DistContext) => Unit):
  (DistIterable[T1], DistIterable[T2], DistIterable[T3], DistIterable[T4],
    DistIterable[T5], DistIterable[T6], DistIterable[T7], DistIterable[T8],
    DistIterable[T9], DistIterable[T10]) = {

    val rs = distDo(distOp, genTypes(10))

    (rs(0).asInstanceOf[DistIterable[T1]], rs(1).asInstanceOf[DistIterable[T2]], rs(2).asInstanceOf[DistIterable[T3]],
      rs(3).asInstanceOf[DistIterable[T4]], rs(4).asInstanceOf[DistIterable[T5]], rs(5).asInstanceOf[DistIterable[T6]],
      rs(6).asInstanceOf[DistIterable[T7]], rs(7).asInstanceOf[DistIterable[T8]], rs(8).asInstanceOf[DistIterable[T9]],
      rs(9).asInstanceOf[DistIterable[T10]])
  }

  def distDo[B](distOp: (T, Emitter[B], DistContext) => Unit): DistIterable[B] =
    distDo(distOp, genTypes(1))(0).asInstanceOf[DistIterable[B]]

  def distDo[B](distOp: (T, Emitter[B]) => Unit): DistIterable[B] =
    distDo((el: T, em: Emitter[B], dc: DistContext) => distOp(el, em), genTypes(1))(0).asInstanceOf[DistIterable[B]]

  def distDo[T1, T2](distOp: (T, Emitter2[T1, T2], DistContext) => Unit): (DistIterable[T1], DistIterable[T2]) = {
    val rs = distDo(distOp, genTypes(2))
    (rs(0).asInstanceOf[DistIterable[T1]], rs(1).asInstanceOf[DistIterable[T2]])
  }

  def flatten[B >: T](distIterable1: DistIterable[B]): DistIterable[T] = flatten(List(distIterable1))

  def flatten[B >: T, C >: T](distIterable1: DistIterable[B], distIterable2: DistIterable[C]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2))

  def flatten[B >: T, C >: T, D >: T](distIterable1: DistIterable[B], distIterable2: DistIterable[C],
                                      distIterable3: DistIterable[D]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2, distIterable3))

  def flatten[B >: T, C >: T, D >: T, E >: T](distIterable1: DistIterable[B], distIterable2: DistIterable[C],
                                              distIterable3: DistIterable[D], distIterable4: DistIterable[E]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2, distIterable3, distIterable4))

  private[this] def genTypes(num: Int) = (1 to num).map(v => (CollectionId(DCUtil.generateNewCollectionURI), manifest[Any]))
}