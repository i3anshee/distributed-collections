package scala.collection.distributed

import api.{DistContext, Emitter}
import collection.immutable.{GenIterable, GenTraversable}
import java.net.URI

trait DistProcessable[+T] {

  // TODO (VJ) Investigate the List of outputs as an option for distributedDo. That would enable creation of a collection of
  // containing distributed collections

  // def distributedDo[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]
  // (parOperation: (T, Emitter[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10], DistContext) => Unit):
  // (DistIterable[T1], DistIterable[T2], DistIterable[T3], DistIterable[T4],
  //  DistIterable[T5], DistIterable[T6], DistIterable[T7], DistIterable[T8],
  //  DistIterable[T9], DistIterable[T10]) // add manifests and number of types

  def sgbr[S, K, T1, T2, That](by: (T) => Ordered[S] = nullOrdered,
                               key: (T, Emitter[T1]) => K = nullKey,
                               reduceOp: (T2, T1) => T2 = nullReduce)
                              (implicit sgbrResult: ToSGBRColl[T, K, T1, T2, That]): That

  def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T]

  def parallelDo[B](parOperation: (T, Emitter[B], DistContext) => Unit): DistIterable[B]

  def groupBy[K, B](keyFunction: (T, Emitter[B]) => K): DistMap[K, GenIterable[B]]

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


  protected[this] val nullOrdered = (el: T) => null

  protected[this] val nullKey = (el: T, em: Emitter[Dummy2]) => Dummy1

  protected[this] val nullReduce = (ag: Dummy3, v: Dummy2) => Dummy3
}

object DistProcessable {
  implicit def sortOnly[T]: ToSGBRColl[T, Dummy1, Dummy2, Dummy3, DistIterable[T]] = new ToSColl[T]

  implicit def sortGroupBy[T, K, T1]: ToSGBRColl[T, K, T1, Dummy3, DistMap[K, GenIterable[T1]]] = new ToGBColl[T, K, T1]

  implicit def sortGroupByCombine[T, K, T1, T2]: ToSGBRColl[T, K, T1, T2, DistMap[K, T2]] = new ToGBRColl[T, K, T1, T2]
}

trait ToSGBRColl[-T, -K, -T1, -T2, +To] {
  def result(uri: URI): To
}

class ToSColl[T] extends ToSGBRColl[T, Dummy1, Dummy2, Dummy3, DistIterable[T]] {
  def result(uri: URI): DistIterable[T] = new DistColl[T](uri)
}

class ToGBColl[T, K, T1] extends ToSGBRColl[T, K, T1, Dummy3, DistMap[K, GenIterable[T1]]] {
  def result(uri: URI): DistMap[K, GenIterable[T1]] = new DistHashMap[K, GenIterable[T1]](uri)
}

class ToGBRColl[T, K, T1, T2] extends ToSGBRColl[T, K, T1, T2, DistMap[K, T2]] {
  def result(uri: URI): DistMap[K, T2] = new DistHashMap[K, T2](uri)
}

trait Dummy1

object Dummy1 extends Dummy1

trait Dummy2

object Dummy2 extends Dummy2

trait Dummy3

object Dummy3 extends Dummy3


