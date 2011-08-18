package scala.collection.distributed

import api.shared.DistBuilderLike
import api.{ReifiedDistCollection, RecordNumber}
import scala.collection.generic.CanBuildFrom
import execution.{ExecutionPlan}
import shared.{DistCounter, DistRecordCounter, DistIterableBuilder}
import collection.{GenIterable, GenTraversableOnce, GenIterableLike, IterableLike}

// TODO (VJ) introduce dependencies to the backend
// TODO (VJ) group by issue with views
// TODO (VJ) extract counters as well as builders
// TODO (VJ) LOW fix the isEmpty check (some frameworks might not support the O(1) isEmpty and size operations).
trait DistIterableLike[+T, +Repr <: DistIterable[T], +Sequential <: Iterable[T] with IterableLike[T, Sequential]]
  extends GenIterableLike[T, Repr]
  with HasNewDistBuilder[T, Repr]
  with RichDistProcessable[T] {

  self: DistIterableLike[T, Repr, Sequential] =>

  protected[this] def isView: Boolean

  protected[this] def inMemorySeq = seq

  protected def seqAndDelete[Elem](): Sequential = inMemorySeq

  def seq: Sequential

  protected[this] def execute(collections: ReifiedDistCollection*): Unit = if (!isView) forceExecute(collections: _*)

  protected[this] def execute: Unit = if (!isView) forceExecute

  protected[this] def forceExecute(collections: ReifiedDistCollection*): Unit = ExecutionPlan.execute(collections)

  protected[this] def forceExecute: Unit = ExecutionPlan.execute

  def view: DistIterableView[T, Repr, Sequential]

  protected[this] def newDistBuilder: DistBuilderLike[T, Repr]

  def repr: Repr = this.asInstanceOf[Repr]

  def hasDefiniteSize = true

  def canEqual(other: Any) = true

  def mkString(start: String, sep: String, end: String): String = seq.mkString(start, sep, end)

  def mkString(sep: String): String = seq.mkString("", sep, "")

  def mkString: String = seq.mkString

  def map[S, That](f: T => S)(implicit bf: CanBuildFrom[Repr, S, That]): That = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, S, That]](repr)
    distForeach(el => rb += f(el), DistIterableBuilder.extractBuilders(f) :+ rb)
    execute(rb)
    rb.result
  }

  def flatMap[S, That](f: (T) => GenTraversableOnce[S])(implicit bf: CanBuildFrom[Repr, S, That]): That = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, S, That]](repr)
    foreach(el => f(el).foreach(v => rb += v))
    execute(rb)
    rb.result
  }

  def filter(p: T => Boolean): Repr = {
    val rb = newDistBuilder.uniqueElementsBuilder
    foreach(el => if (p(el)) rb += el)
    execute(rb)
    rb.result
  }

  def filterNot(pred: (T) => Boolean) = filter(!pred(_))

  def groupBySeq[K](f: (T) => K): DistMap[K, GenIterable[T]] = groupByKey((v: T) => (f(v), v))

  def groupByKey[K, V](implicit ev: <:<[T, (K, V)]): DistMap[K, GenIterable[V]] = groupByKey(v => v)

  def reduce[A1 >: T](op: (A1, A1) => A1) = if (isEmpty)
    throw new UnsupportedOperationException("empty.reduce")
  else {
    val result = map(v => (true, v)).combine((it: Iterable[T]) => it.reduce(op))
    forceExecute(result)
    result.seqAndDelete.head._2
  }

  def find(pred: (T) => Boolean) = if (isEmpty)
    None
  else {
    var found = false
    val recordCount = DistRecordCounter()
    val allResults = DistIterableBuilder[(RecordNumber, T)]()
    foreach(el => if (!found && pred(el)) {
      allResults += ((recordCount.recordNumber, el))
      found = true
    })

    val res = allResults.result
    forceExecute(res)
    val allResultsSeq = res.seqAndDelete.toSeq
    if (allResultsSeq.size == 0)
      None
    else
    // sort by record number and pick first
      Some(allResultsSeq.toSeq.sorted(Ordering[RecordNumber].on[(RecordNumber, _)](_._1)).head._2)
  }

  def partition(pred: (T) => Boolean) = {
    val b1 = newDistBuilder.uniqueElementsBuilder
    val b2 = newDistBuilder.uniqueElementsBuilder

    foreach(v => (if (pred(v)) b1 else b2) += v)
    execute(b1, b2)

    (b1.result(), b2.result())
  }

  def collect[B, That](pf: PartialFunction[T, B])(implicit bf: CanBuildFrom[Repr, B, That]) = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, B, That]](repr).uniqueElementsBuilder
    foreach(el => if (pf.isDefinedAt(el)) rb += pf(el))
    execute(rb)

    rb.result()
  }

  def ++[B >: T, That](that: GenTraversableOnce[B])(implicit bf: CanBuildFrom[Repr, B, That]): That = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, T, That]](repr)
    val collection = flatten(that.asInstanceOf[DistIterable[B]])
    execute(collection)
    rb.result(collection.location)
  }

  def reduceOption[A1 >: T](op: (A1, A1) => A1) = if (isEmpty) None else Some(reduce(op))

  def count(p: (T) => Boolean) = countLong(p).toInt

  def countLong(p: (T) => Boolean): Long = {
    val cnt = DistCounter()
    foreach(v => if (p(v)) cnt += 1)
    //TODO (VJ) Dependency on counters in execution plan
    forceExecute
    cnt()
  }

  def forall(pred: (T) => Boolean) = {
    var found = false
    val builder = DistIterableBuilder[Boolean]

    foreach(el => if (!found && !pred(el)) {
      builder += true
      found = true
    })

    val res = builder.result
    forceExecute(res)
    res.seqAndDelete.size > 0
  }

  def exists(pred: (T) => Boolean) = {
    var found = false
    val counter = DistCounter()

    foreach(el => if (!found && pred(el)) counter += 1)

    forceExecute()
    counter() > 0
  }

  def fold[A1 >: T](z: A1)(op: (A1, A1) => A1) = if (!isEmpty) op(reduce(op), z) else z

  def toSet[A1 >: T] = seq.toSet

  def foldRight[B](z: B)(op: (T, B) => B) = seq.foldRight(z)(op)

  def :\[B](z: B)(op: (T, B) => B): B = foldRight(z)(op)

  def foldLeft[B](z: B)(op: (B, T) => B) = seq.foldLeft(z)(op)

  def /:[B](z: B)(op: (B, T) => B): B = foldLeft(z)(op)

  def reduceRightOption[B >: T](op: (T, B) => B) = seq.reduceRightOption(op)

  def reduceLeftOption[B >: T](op: (B, T) => B) = seq.reduceLeftOption(op)

  def reduceRight[B >: T](op: (T, B) => B) = seq.reduceRight(op)

  def toMap[K, V](implicit ev: <:<[T, (K, V)]) = seq.toMap

  def toSeq = seq.toSeq

  def toIterable = seq.toIterable

  def toTraversable = seq.toTraversable

  def toBuffer[A1 >: T] = seq.toBuffer

  def toIterator = iterator

  def toStream = seq.toStream

  def toIndexedSeq[A1 >: T] = seq.toIndexedSeq

  def toList = seq.toList

  def toArray[A1 >: T](implicit evidence$1: ClassManifest[A1]) = seq.toArray(evidence$1)

  def foreach[U](f: (T) => U) = distForeach(f, DistIterableBuilder.extractBuilders(f))

  def iterator = seq.toIterable.iterator

  def combineValues[B, V1 >: T, V2 <: V1](cmbOp: (Iterable[V1]) => V2): V2 = cmbOp(map(v => (true, v)).combine(cmbOp).seq.unzip._2)

  def aggregate[B](z: B)(seqop: (B, T) => B, combop: (B, B) => B) = {
    map(v => (true, v)).combine((it: Iterable[T]) => it.foldLeft(z)(seqop)).seq.unzip._2.reduce(combop)
  }

  def minBy[B](f: (T) => B)(implicit cmp: Ordering[B]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def maxBy[B](f: (T) => B)(implicit cmp: Ordering[B]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def min[A1 >: T](implicit ord: Ordering[A1]) = combineValues((it: Iterable[T]) => it.min(ord))

  def max[A1 >: T](implicit ord: Ordering[A1]) = combineValues((it: Iterable[T]) => it.max(ord))

  def product[A1 >: T](implicit num: Numeric[A1]) = combineValues((it: Iterable[A1]) => it.product(num))

  def sum[A1 >: T](implicit num: Numeric[A1]) = combineValues((it: Iterable[A1]) => it.sum(num))

  def copyToArray[B >: T](xs: Array[B]) = copyToArray(xs, 0)

  def copyToArray[B >: T](xs: Array[B], start: Int) = copyToArray(xs, start, xs.length - start)

  def copyToArray[B >: T](xs: Array[B], start: Int, len: Int) = seq.copyToArray(xs, start, len)

  protected[this] def bf2seq[S, That](bf: CanBuildFrom[Repr, S, That]) = new CanBuildFrom[Sequential, S, That] {
    def apply(from: Sequential) = bf.apply(from.asInstanceOf[Repr])

    def apply() = bf.apply()
  }

  def scanLeft[S, That](z: S)(op: (S, T) => S)(implicit bf: CanBuildFrom[Repr, S, That]) = seq.scanLeft(z)(op)(bf2seq(bf))

  def scanRight[S, That](z: S)(op: (T, S) => S)(implicit bf: CanBuildFrom[Repr, S, That]) = seq.scanRight(z)(op)(bf2seq(bf))

  def drop(n: Int) = {
    val rb = newDistBuilder.uniqueElementsBuilder
    throw new UnsupportedOperationException("Not implemented yet!!!")
  }

  def zipWithLongIndex[A1 >: T, That](implicit bf: CanDistBuildFrom[Repr, (A1, Long), That]) = throw new UnsupportedOperationException("Not implemented yet!!!")
//  {
//    val rb = bf(repr).uniqueElementsBuilder
//
//    make one collection of (filePart, el)
//    val recordCounter = DistRecordCounter()
//    val recordNumbers = DistIterableBuilder[(Long, A1)]()
//    val records = map((el => {
//      recordNumbers += ((recordCounter.recordNumber.filePart, recordCounter.counter))
//      (recordCounter.recordNumber, el)
//    })
//
//    val maximums = recordNumbers.result.combine(it => it.max)
//    val maximumMap = maximums.delete(recordNumbers, it => {
//      val (fileParts: immutable.GenSeq[Long], records: immutable.GenSeq[Long]) = it.toSeq.sortWith(_._1 < _._1).unzip
//      fileParts.zip(records.scanLeft(0L)(_ + _))
//    })
//
//    records.result.map(el => (v, maximumMap.get(el._1).get + ctx.recordNumber.counter))
//  }

  def zipWithIndex[A1 >: T, That](implicit bf: CanBuildFrom[Repr, (A1, Int), That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def scan[B >: T, That](z: B)(op: (B, B) => B)(implicit cbf: CanBuildFrom[Repr, B, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def dropWhile(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def span(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def splitAt(n: Int) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def takeWhile(pred: (T) => Boolean) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def take(n: Int) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def slice(unc_from: Int, unc_until: Int) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def zipAll[B, A1 >: T, That](that: collection.GenIterable[B], thisElem: A1, thatElem: B)(implicit bf: CanBuildFrom[Repr, (A1, B), That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def zip[A1 >: T, B, That](that: collection.GenIterable[B])(implicit bf: CanBuildFrom[Repr, (A1, B), That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def sameElements[A1 >: T](that: collection.GenIterable[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def groupBy[K](f: (T) => K) = throw new UnsupportedOperationException("Not implemented yet!!!")
}
