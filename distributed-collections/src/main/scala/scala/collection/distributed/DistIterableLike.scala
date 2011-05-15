package scala.collection.distributed

import api.Emitter
import scala.collection.generic.{CanBuildFrom}
import scala._
import collection.immutable
import collection.{GenTraversableOnce, GenIterableLike}
import immutable.GenIterable
import execution.ExecutionPlan


trait DistIterableLike[+T, +Repr <: DistIterable[T], +Sequential <: Iterable[T] with GenIterableLike[T, Sequential]]
  extends GenIterableLike[T, Repr]
  with HasNewRemoteBuilder[T, Repr]
  with RichDistProcessable[T] {

  self: DistIterableLike[T, Repr, Sequential] =>

  protected[this] def newRemoteBuilder: RemoteBuilder[T, Repr]

  def repr: Repr = this.asInstanceOf[Repr]

  def hasDefiniteSize = true

  def canEqual(other: Any) = true

  def mkString(start: String, sep: String, end: String): String = seq.mkString(start, sep, end)

  def mkString(sep: String): String = seq.mkString("", sep, "")

  def mkString: String = seq.mkString

  override def toString = seq.mkString(stringPrefix + "(", ", ", ")")

  def map[S, That](f: T => S)(implicit bf: CanBuildFrom[Repr, S, That]): That = {
    val remoteBuilder = bf.asInstanceOf[CanDistBuildFrom[Repr, S, That]](repr)
    val collection = distDo((el: T, em: Emitter[S]) => em.emit(f(el)))
    remoteBuilder.result(collection)
  }

  def filter(p: T => Boolean): Repr = {
    val rb = newRemoteBuilder
    rb.uniquenessPreserved

    rb.result(distDo((el: T, em: Emitter[T]) => if (p(el)) em.emit(el)))
  }

  def groupBySeq[K](f: (T) => K) = groupBySort((v: T, em: Emitter[T]) => {
    em.emit(v);
    f(v)
  })

  def foldRight[B](z: B)(op: (T, B) => B) = seq.foldRight(z)(op)

  def :\[B](z: B)(op: (T, B) => B): B = foldRight(z)(op)

  def foldLeft[B](z: B)(op: (B, T) => B) = seq.foldLeft(z)(op)

  def /:[B](z: B)(op: (B, T) => B): B = foldLeft(z)(op)

  def fold[A1 >: T](z: A1)(op: (A1, A1) => A1) = if (!isEmpty) op(reduce(op), z) else z

  def reduceRightOption[B >: T](op: (T, B) => B) = seq.reduceRightOption(op)

  def reduceLeftOption[B >: T](op: (B, T) => B) = seq.reduceLeftOption(op)

  def reduceRight[B >: T](op: (T, B) => B) = seq.reduceRight(op)

  def toMap[K, V](implicit ev: <:<[T, (K, V)]) = seq.toMap

  def toSet[A1 >: T] = seq.toSet

  def toSeq = seq.toSeq

  def toIterable = seq.toIterable

  def toTraversable = seq.toTraversable

  def toBuffer[A1 >: T] = seq.toBuffer

  def toIterator = iterator

  def toStream = seq.toStream

  def toIndexedSeq[A1 >: T] = seq.toIndexedSeq

  def toList = seq.toList

  def toArray[A1 >: T](implicit evidence$1: ClassManifest[A1]) = seq.toArray(evidence$1)

  def foreach[U](f: (T) => U) = seq.foreach(f)

  def reduce[A1 >: T](op: (A1, A1) => A1) = if (isEmpty)
    throw new UnsupportedOperationException("empty.reduce")
  else {
    val result = groupBySort((v: T, emitter: Emitter[T]) => {
      emitter.emit(v);
      1
    }).combine((it: Iterable[T]) => it.reduce(op))
    ExecutionPlan.execute(result)
    result.toTraversable.head._2
    }

  def scanRight[B, That](z: B)(op: (T, B) => B)(implicit bf: CanBuildFrom[Repr, B, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def scanLeft[B, That](z: B)(op: (B, T) => B)(implicit bf: CanBuildFrom[Repr, B, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def groupBy[K](f: (T) => K): DistMap[K, Repr] = throw new UnsupportedOperationException("Not implemented yet!!!")

  def reduceOption[A1 >: T](op: (A1, A1) => A1) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def count(p: (T) => Boolean) = 0

  def drop(n: Int) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def dropWhile(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def span(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def splitAt(n: Int) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def takeWhile(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def take(n: Int) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def slice(unc_from: Int, unc_until: Int) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def minBy[B](f: (T) => B)(implicit cmp: Ordering[B]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def maxBy[B](f: (T) => B)(implicit cmp: Ordering[B]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def max[A1 >: T](implicit ord: Ordering[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def min[A1 >: T](implicit ord: Ordering[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def product[A1 >: T](implicit num: Numeric[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def sum[A1 >: T](implicit num: Numeric[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def aggregate[B](z: B)(seqop: (B, T) => B, combop: (B, B) => B) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def flatMap[B, That](f: (T) => GenTraversableOnce[B])(implicit bf: CanBuildFrom[Repr, B, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def partition(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def filterNot(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def forall(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def ++[B >: T, That](that: GenTraversableOnce[B])(implicit bf: CanBuildFrom[Repr, B, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def find(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def exists(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def collect[B, That](pf: PartialFunction[T, B])(implicit bf: CanBuildFrom[Repr, B, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def scan[B >: T, That](z: B)(op: (B, B) => B)(implicit cbf: CanBuildFrom[Repr, B, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def zipAll[B, A1 >: T, That](that: collection.GenIterable[B], thisElem: A1, thatElem: B)(implicit bf: CanBuildFrom[Repr, (A1, B), That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def copyToArray[B >: T](xs: Array[B], start: Int, len: Int) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def copyToArray[B >: T](xs: Array[B], start: Int) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def copyToArray[B >: T](xs: Array[B]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def iterator = throw new UnsupportedOperationException("Not implemented yet!!!")

  def zipWithIndex[A1 >: T, That](implicit bf: CanBuildFrom[Repr, (A1, Int), That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def zip[A1 >: T, B, That](that: collection.GenIterable[B])(implicit bf: CanBuildFrom[Repr, (A1, B), That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def sameElements[A1 >: T](that: collection.GenIterable[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")
}