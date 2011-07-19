package scala.collection.distributed

import api.shared.DistBuilderLike
import api.{RecordNumber, DistContext, Emitter}
import scala.collection.generic.CanBuildFrom
import scala._
import collection.immutable
import collection.{GenTraversableOnce, GenIterableLike}
import execution.{ExecutionPlan}
import shared.{DistRecordCounter, DistIterableBuilder}

// TODO (VJ) group by issue
// TODO (VJ) extract counters as well as builders
// TODO (VJ) LOW fix the isEmpty check
// TODO (VJ) LOW fix the GenXXX interfaces. If they are sequential or parallel completely different algorithm needs to be used
trait DistIterableLike[+T, +Repr <: DistIterable[T], +Sequential <: Iterable[T] with GenIterableLike[T, Sequential]]
  extends GenIterableLike[T, Repr]
  with HasNewDistBuilder[T, Repr]
  with RichDistProcessable[T] {

  self: DistIterableLike[T, Repr, Sequential] =>

  protected[this] def isView: Boolean

  protected[this] def inMemorySeq = seq

  protected def seqAndDelete[Elem](): Sequential = inMemorySeq

  def seq: Sequential

  protected[this] def execute: Unit = if (!isView) ExecutionPlan.execute(repr)

  protected[this] def forceExecute: Unit = ExecutionPlan.execute(repr)

  def view: DistIterableView[T, Repr, Sequential]

  protected[this] def newDistBuilder: DistBuilderLike[T, Repr]

  def repr: Repr = this.asInstanceOf[Repr]

  def hasDefiniteSize = true

  def canEqual(other: Any) = true

  def mkString(start: String, sep: String, end: String): String = seq.mkString(start, sep, end)

  def mkString(sep: String): String = seq.mkString("", sep, "")

  def mkString: String = seq.mkString

  //TODO (VJ) LOW maybe just write the location of the collection (it is less dangerous)
  override def toString = seq.mkString(stringPrefix + "(", ", ", ")")

  def map[S, That](f: T => S)(implicit bf: CanBuildFrom[Repr, S, That]): That = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, S, That]](repr)
    distForeach(el => rb += f(el), DistIterableBuilder.extractBuilders(f) :+ rb)
    execute
    rb.result
  }

  def flatMap[S, That](f: (T) => GenTraversableOnce[S])(implicit bf: CanBuildFrom[Repr, S, That]): That = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, S, That]](repr)
    foreach(el => f(el).foreach(v => rb += v))
    execute
    rb.result
  }

  def filter(p: T => Boolean): Repr = {
    val rb = newDistBuilder.uniqueElementsBuilder
    foreach(el => if (p(el)) rb += el)
    execute
    rb.result
  }

  def filterNot(pred: (T) => Boolean) = filter(!pred(_))

  def groupBySeq[K](f: (T) => K): DistMap[K, immutable.GenIterable[T]] = groupByKey((v: T) => (f(v), v))

  def groupByKey[K, V](implicit ev: <:<[T, (K, V)]): DistMap[K, immutable.GenIterable[V]] = groupByKey(v => v)

  def reduce[A1 >: T](op: (A1, A1) => A1) = if (isEmpty)
    throw new UnsupportedOperationException("empty.reduce")
  else {
    val result = map(v => (true, v)).combine((it: Iterable[T]) => it.reduce(op))
    ExecutionPlan.execute(result)
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
    ExecutionPlan.execute(res)
    val allResultsSeq = res.seqAndDelete.toSeq
    if (allResultsSeq.size == 0)
      None
    else
    // sort by record number and pick first
      Some(allResultsSeq.toSeq.sorted(Ordering[RecordNumber].on[(RecordNumber, _)](_._1)).head._2)
  }

  def partition(pred: (T) => Boolean) = {
    val builder1 = newDistBuilder.uniqueElementsBuilder
    val builder2 = newDistBuilder.uniqueElementsBuilder

    // partition
    foreach(v => (if (pred(v)) builder1 else builder2) += v)

    // fetch results
    (builder1.result(), builder2.result())
  }

  def collect[B, That](pf: PartialFunction[T, B])(implicit bf: CanBuildFrom[Repr, B, That]) = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, B, That]](repr).uniqueElementsBuilder
    foreach(el => if (pf.isDefinedAt(el)) rb += pf(el))
    rb.result()
  }

  def ++[B >: T, That](that: GenTraversableOnce[B])(implicit bf: CanBuildFrom[Repr, B, That]): That = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, T, That]](repr)
    val collection = flatten(that.asInstanceOf[DistIterable[B]])
    rb.result(collection.location)
  }

  def reduceOption[A1 >: T](op: (A1, A1) => A1) = if (isEmpty) None else Some(reduce(op))

  def count(p: (T) => Boolean) = 0

  def forall(pred: (T) => Boolean) = {
    var found = false
    val builder = DistIterableBuilder[Boolean]

    foreach(el => if (!found && !pred(el)) {
      builder += true
      found = true
    })

    val res = builder.result
    ExecutionPlan.execute(res)
    res.seqAndDelete.size > 0
  }

  def exists(pred: (T) => Boolean) = {
    var found = false
    val builder = DistIterableBuilder[Boolean]

    foreach(el => if (!found && pred(el)) {
      builder += true
      found = true
    })

    val res = builder.result()
    ExecutionPlan.execute(res)
    res.seqAndDelete.size > 0
  }

  def fold[A1 >: T](z: A1)(op: (A1, A1) => A1) = if (!isEmpty) op(reduce(op), z) else z

  // Implemented with seq collection
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

  def combineValues[B, That](cmbOp: (Iterable[T]) => B)(implicit bf: CanBuildFrom[Repr, B, That]): That = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, B, That]](repr)
    val res = map(v => (true, v)).combine(cmbOp)
    rb.result(res.location)
  }

  def minBy[B](f: (T) => B)(implicit cmp: Ordering[B]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def maxBy[B](f: (T) => B)(implicit cmp: Ordering[B]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //  {
  //    val result = groupBySort((v: T, emitter: Emitter[T]) => {
  //      emitter.emit(v);
  //      1
  //    }).combine((it: Iterable[T]) => it.maxBy(f)(cmp))
  //
  //    ExecutionPlan.execute(result)
  //
  //    result.seqAndDelete.head._2
  //  }

  def max[A1 >: T](implicit ord: Ordering[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //  {
  //    val result = groupBySort((v: T, emitter: Emitter[T]) => {
  //      emitter.emit(v);
  //      1
  //    }).combine((it: Iterable[T]) => it.max(ord))
  //
  //    ExecutionPlan.execute(result)
  //
  //    result.seqAndDelete.head._2
  //  }

  def min[A1 >: T](implicit ord: Ordering[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //  {
  //    val result = groupBySort((v: T, emitter: Emitter[T]) => {
  //      emitter.emit(v);
  //      1
  //    }).combine((it: Iterable[T]) => it.min(ord))
  //
  //    ExecutionPlan.execute(result)
  //
  //    result.seqAndDelete.head._2
  //  }

  def product[A1 >: T](implicit num: Numeric[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //  {
  //    val result = groupBySort((v: T, emitter: Emitter[T]) => {
  //      emitter.emit(v);
  //      1
  //    }).combine((it: Iterable[T]) => it.product(num))
  //
  //    ExecutionPlan.execute(result)
  //
  //    result.seqAndDelete.head._2
  //  }

  def sum[A1 >: T](implicit num: Numeric[A1]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //  {
  //    val result = groupBySort((v: T, emitter: Emitter[T]) => {
  //      emitter.emit(v);
  //      1
  //    }).combine((it: Iterable[T]) => it.sum(num))
  //
  //    ExecutionPlan.execute(result)
  //
  //    result.seqAndDelete.head._2
  //  }

  protected[this] def bf2seq[S, That](bf: CanBuildFrom[Repr, S, That]) = new CanBuildFrom[Sequential, S, That] {
    def apply(from: Sequential) = bf.apply(from.asInstanceOf[Repr])

    def apply() = bf.apply()
  }

  def copyToArray[B >: T](xs: Array[B]) = copyToArray(xs, 0)

  def copyToArray[B >: T](xs: Array[B], start: Int) = copyToArray(xs, start, xs.length - start)

  def copyToArray[B >: T](xs: Array[B], start: Int, len: Int) = seq.copyToArray(xs, start, len)

  def scanLeft[S, That](z: S)(op: (S, T) => S)(implicit bf: CanBuildFrom[Repr, S, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //seq.scanLeft(z)(op)(bf2seq(bf))

  def scanRight[S, That](z: S)(op: (T, S) => S)(implicit bf: CanBuildFrom[Repr, S, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //seq.scanRight(z)(op)(bf2seq(bf))

  // TODO (vj) optimize when partitioning is introducedS
  // TODO (vj) for now use only shared collection.
  def drop(n: Int) = {
    val rb = newDistBuilder.uniqueElementsBuilder
    // max of records sorted by file part
    //    val sizes = new DSECollection[(Long, Long)](
    //      Some(_.foldLeft((0L, 0L))((aggr, v) => (v._1, scala.math.max(aggr._2, v._2)))),
    //      Some(it => {
    //        val (fileParts: immutable.GenSeq[Long], records: immutable.GenSeq[Long]) = it.toSeq.sortWith(_._1 < _._1).unzip
    //        fileParts.zip(records.scanLeft(0L)(_ + _))
    //      })
    //    )
    throw new UnsupportedOperationException("Not implemented yet!!!")
  }

  def zipWithLongIndex[A1 >: T, That](implicit bf: CanDistBuildFrom[Repr, (A1, Long), That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //  {
  //    val rb = bf(repr)
  //    rb.uniquenessPreserved
  // TODO (vj) optimize when partitioning is introduced
  // TODO (vj) for now use only shared collection. If needed introduce SharedMap

  //    val (records, recordNumbers) = distDo((el: A1, em: Emitter2[(Long, A1)], ctx: DistContext) => {
  //      em.emit((ctx.recordNumber.filePart, el))
  //      em.emit1(((ctx.recordNumber.filePart, ctx.recordNumber.counter)))
  //    })
  //
  //    val maximums = recordNumbers.combine(_.foldLeft((0L, 0L))((aggr, v) => (v._1, scala.math.max(aggr._2, v._2))))
  //    val maximumMap = DefObject[Map[Long, Long]](recordNumbers, it => {
  //      val (fileParts: immutable.GenSeq[Long], records: immutable.GenSeq[Long]) = it.toSeq.sortWith(_._1 < _._1).unzip
  //      fileParts.zip(records.scanLeft(0L)(_ + _))
  //    })
  //
  //    val zippedWithIndex = records.distDo((el: (Long, A1), em: Emitter[(A1, Long)], ctx: DistContext) =>
  //      em.emit((v, maximumMap.get(el._1).get + ctx.recordNumber.counter))
  //    )
  //
  //    rb.result(zippedWithIndex)
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

  def aggregate[B](z: B)(seqop: (B, T) => B, combop: (B, B) => B) = throw new UnsupportedOperationException("Not implemented yet!!!")

  def groupBy[K](f: (T) => K) = throw new UnsupportedOperationException("Not implemented yet!!!")
}
