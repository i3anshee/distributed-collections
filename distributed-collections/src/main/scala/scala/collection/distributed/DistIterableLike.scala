package scala.collection.distributed

import api.Emitter
import scala.collection.generic.{CanBuildFrom}
import scala.collection.{GenIterable, GenIterableLike}
import scala._

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

trait DistIterableLike[+T, +Repr <: DistIterable[T], +Sequential <: Iterable[T] with GenIterableLike[T, Sequential]]
  extends GenIterableLike[T, Repr]
  with HasNewRemoteBuilder[T, Repr]
  with DistProcessFramework[T]
{
  self: DistIterableLike[T, Repr, Sequential] =>

  def seq: Sequential = throw new UnsupportedOperationException("Not implemented yet")

  protected[this] def newRemoteBuilder: RemoteBuilder[T, Repr]

  def repr: Repr = this.asInstanceOf[Repr]

  def hasDefiniteSize = true

  def nonEmpty = size != 0

  def iterator = seq.iterator

  def mkString(start: String, sep: String, end: String): String = seq.mkString(start, sep, end)

  def mkString(sep: String): String = seq.mkString("", sep, "")

  def mkString: String = seq.mkString("")

  override def toString = seq.mkString(stringPrefix + "(", ", ", ")")

  def map[S, That](f: T => S)(implicit bf: CanBuildFrom[Repr, S, That]): That = {
    val remoteBuilder = bf.asInstanceOf[CanDistBuildFrom[Repr, S, That]](repr)
    val collection = parallelDo((el: T, em: Emitter[S]) => em.emit(f(el)))
    remoteBuilder.result(collection)
  }

  def filter(p: T => Boolean): Repr = {
    val rb = newRemoteBuilder
    rb.uniquenessPreserved

    rb.result(parallelDo((el: T, em: Emitter[T]) => if (p(el)) em.emit(el)))
  }

  def toMap[K, V](ev: <:<[T, (K, V)]) = throw new UnsupportedOperationException("Not implemented yet")

  def toSet[A1 >: T] = throw new UnsupportedOperationException("Not implemented yet")

  def toSeq = throw new UnsupportedOperationException("Not implemented yet")

  def toIterable = throw new UnsupportedOperationException("Not implemented yet")

  def toTraversable = throw new UnsupportedOperationException("Not implemented yet")

  def toBuffer[A1 >: T] = throw new UnsupportedOperationException("Not implemented yet")

  def toIterator = throw new UnsupportedOperationException("Not implemented yet")

  def toStream = throw new UnsupportedOperationException("Not implemented yet")

  def toIndexedSeq[A1 >: T] = throw new UnsupportedOperationException("Not implemented yet")

  def toList = throw new UnsupportedOperationException("Not implemented yet")

  def toArray[A1 >: T](evidence$1: ClassManifest[A1]) = throw new UnsupportedOperationException("Not implemented yet")

  def copyToArray[B >: T](xs: Array[B], start: Int, len: Int) = throw new UnsupportedOperationException("Not implemented yet")

  def copyToArray[B >: T](xs: Array[B], start: Int) = throw new UnsupportedOperationException("Not implemented yet")

  def copyToArray[B >: T](xs: Array[B]) = throw new UnsupportedOperationException("Not implemented yet")

  def exists(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet")

  def forall(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet")

  def minBy[B](f: (T) => B)(cmp: Ordering[B]) = throw new UnsupportedOperationException("Not implemented yet")

  def maxBy[B](f: (T) => B)(cmp: Ordering[B]) = throw new UnsupportedOperationException("Not implemented yet")

  def max[A1 >: T](ord: Ordering[A1]) = throw new UnsupportedOperationException("Not implemented yet")

  def min[A1 >: T](ord: Ordering[A1]) = throw new UnsupportedOperationException("Not implemented yet")

  def product[A1 >: T](num: Numeric[A1]) = throw new UnsupportedOperationException("Not implemented yet")

  def sum[A1 >: T](num: Numeric[A1]) = throw new UnsupportedOperationException("Not implemented yet")

  def count(p: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet")

  def reduceRightOption[B >: T](op: (T, B) => B) = throw new UnsupportedOperationException("Not implemented yet")

  def reduceLeftOption[B >: T](op: (B, T) => B) = throw new UnsupportedOperationException("Not implemented yet")

  def reduceRight[B >: T](op: (T, B) => B) = throw new UnsupportedOperationException("Not implemented yet")

  def aggregate[B](z: B)(seqop: (B, T) => B, combop: (B, B) => B) = throw new UnsupportedOperationException("Not implemented yet")

  def foldRight[B](z: B)(op: (T, B) => B) = throw new UnsupportedOperationException("Not implemented yet")

  def foldLeft[B](z: B)(op: (B, T) => B) = throw new UnsupportedOperationException("Not implemented yet")

  def :\[B](z: B)(op: (T, B) => B) = throw new UnsupportedOperationException("Not implemented yet")

  def /:[B](z: B)(op: (B, T) => B) = throw new UnsupportedOperationException("Not implemented yet")

  def fold[A1 >: T](z: A1)(op: (A1, A1) => A1) = throw new UnsupportedOperationException("Not implemented yet")

  def reduceOption[A1 >: T](op: (A1, A1) => A1) = throw new UnsupportedOperationException("Not implemented yet")

  def reduce[A1 >: T](op: (A1, A1) => A1) = throw new UnsupportedOperationException("Not implemented yet")

  protected[this] def parCombiner = throw new UnsupportedOperationException("Not implemented yet")

  def find(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet")

  def zipAll[B, A1 >: T, That](that: GenIterable[B], thisElem: A1, thatElem: B)(bf: CanBuildFrom[Repr, (A1, B), That]) = throw new UnsupportedOperationException("Not implemented yet")

  def zipWithIndex[A1 >: T, That](bf: CanBuildFrom[Repr, (A1, Int), That]) = throw new UnsupportedOperationException("Not implemented yet")

  def zip[A1 >: T, B, That](that: GenIterable[B])(bf: CanBuildFrom[Repr, (A1, B), That]) = throw new UnsupportedOperationException("Not implemented yet")

  def sameElements[A1 >: T](that: GenIterable[A1]) = throw new UnsupportedOperationException("Not implemented yet")

}