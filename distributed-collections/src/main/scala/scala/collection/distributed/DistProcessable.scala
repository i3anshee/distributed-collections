package scala.collection.distributed

import api._
import java.net.URI
import collection.immutable.{GenSeq, GenIterable, GenTraversable}


/**
 * Base trait for collection operations based on shared nothing processing frameworks (Hadoop, Google MapReduce and Dryad).
 */
trait DistProcessable[+T] {

  def distDo(distOp: (T, UntypedEmitter, DistContext) => Unit, outputs: GenSeq[(CollectionId, Manifest[_])]): GenSeq[DistIterable[Any]]

  def sgbr[S, K, T1, T2, That](by: (T) => Ordered[S] = NullOrdered,
                               key: (T, Emitter[T1]) => K = NullKey,
                               reduceOp: (T2, T1) => T2 = nullReduce)
                              (implicit sgbrResult: ToSGBRColl[T, K, T1, T2, That]): That

  def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T]


  protected[this] val NullOrdered = (el: T) => null

  protected[this] val NullKey = (el: T, em: Emitter[Dummy2]) => Dummy1

  protected[this] val NullReduce = (ag: Dummy3, v: Dummy2) => Dummy3

  protected[this] def nullReduce[D] = NullReduce.asInstanceOf[(Dummy3, D) => Dummy3]

}

trait ToSGBRColl[-T, -K, -T1, -T2, +To] {
  def result(uri: URI): To
}

object ToSGBRColl {
  implicit def sortOnly[T]: ToSGBRColl[T, Dummy1, Dummy2, Dummy3, DistIterable[T]] = new ToSColl[T]

  implicit def sortGroupBy[T, K, T1]: ToSGBRColl[T, K, T1, Dummy3, DistMap[K, GenIterable[T1]]] = new ToGBColl[T, K, T1]

  implicit def sortGroupByCombine[T, K, T1, T2 ]: ToSGBRColl[T, K, T1, T2, DistMap[K, T2]] = new ToGBRColl[T, K, T1, T2]
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


