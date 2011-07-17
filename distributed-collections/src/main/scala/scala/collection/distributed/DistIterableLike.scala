package scala.collection.distributed

import api.shared.DistBuilderLike
import api.{RecordNumber, DistContext, Emitter}
import scala.collection.generic.CanBuildFrom
import scala._
import collection.immutable
import collection.{GenTraversableOnce, GenIterableLike}
import execution.{ExecutionPlan}
import shared.{DistRecordCounter, DistIterableBuilder}

// TODO (VJ) HIGH fix the uniqueness preserved
// TODO (VJ) HIGH consolidate ExecutionPlan.execute
// TODO (VJ) LOW fix the isEmpty check
// TODO (VJ) LOW fix the GenXXX interfaces. If they are sequential or parallel completely different algorithm needs to be used
trait DistIterableLike[+T, +Repr <: DistIterable[T], +Sequential <: immutable.Iterable[T] with GenIterableLike[T, Sequential]]
  extends GenIterableLike[T, Repr]
  with HasNewDistBuilder[T, Repr]
  with RichDistProcessable[T] {

  self: DistIterableLike[T, Repr, Sequential] =>

  def isView: Boolean

  def seq: Sequential

  protected def execute: Unit = if (!isView) ExecutionPlan.execute(repr)

  def view: DistIterableLike[T, Repr, Sequential] = throw new UnsupportedOperationException("Implementation in progress!!!")

  //TODO (VJ) fix this issue
  protected[this] def bf2seq[S, That](bf: CanBuildFrom[Repr, S, That]) = new CanBuildFrom[Sequential, S, That] {
    def apply(from: Sequential) = bf.apply(from.asInstanceOf[Repr])

    def apply() = bf.apply()
  }

  protected[this] def newDistBuilder: DistBuilderLike[T, Repr]

  def repr: Repr = this.asInstanceOf[Repr]

  def hasDefiniteSize = true

  def canEqual(other: Any) = true

  def mkString(start: String, sep: String, end: String): String = seq.mkString(start, sep, end)

  def mkString(sep: String): String = seq.mkString("", sep, "")

  def mkString: String = seq.mkString

  override def toString = seq.mkString(stringPrefix + "(", ", ", ")")

  def map[S, That](f: T => S)(implicit bf: CanBuildFrom[Repr, S, That]): That = {
    val builder = bf.asInstanceOf[CanDistBuildFrom[Repr, S, That]](repr)
    distForeach(el => builder += f(el), DistIterableBuilder.extractBuilders(f) :+ builder)
    execute
    builder.result()
  }

  def flatMap[S, That](f: (T) => GenTraversableOnce[S])(implicit bf: CanBuildFrom[Repr, S, That]): That = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, S, That]](repr)
    foreach(el => f(el).foreach(v => rb += v))
    rb.result()
  }

  def filter(p: T => Boolean): Repr = {
    val rb = newDistBuilder
    //    rb.uniquenessPreserved
    foreach(el => if (p(el)) rb += el)
    rb.result
  }

  def filterNot(pred: (T) => Boolean) = filter(!pred(_))

  def groupBySeq[K](f: (T) => K): DistMap[K, immutable.GenIterable[T]] with DistCombinable[K, T] = groupBySort((v: T, em: Emitter[T]) => {
    em.emit(v);
    f(v)
  })

  def groupByKey[K, V](implicit ev: <:<[T, (K, V)]): DistMap[K, immutable.GenIterable[V]] with DistCombinable[K, V] = {
    groupBySort((v: T, em: Emitter[V]) => {
      em.emit(v._2)
      v._1
    })
  }

  // TODO (VJ) delete the result
  def reduce[A1 >: T](op: (A1, A1) => A1) = if (isEmpty)
    throw new UnsupportedOperationException("empty.reduce")
  else {
    val result = groupBySeq(v => true).combine((it: Iterable[T]) => it.reduce(op))
    ExecutionPlan.execute(result)
    result.toTraversable.head._2
  }

  //TODO (VJ) delete intermediary result
  //TODO (VJ) fix the record counter
  def find(pred: (T) => Boolean) = if (isEmpty)
    None
  else {
    var found = false;
    val recordCount = DistRecordCounter()
    val allResults = DistIterableBuilder[(RecordNumber, T)]()
    foreach(el => if (!found && pred(el)) {
      allResults += ((recordCount.recordNumber, el))
      found = true;
    })

    ExecutionPlan.execute(allResults.result)
    val allResultsSeq = allResults.result.seq.toSeq
    if (allResultsSeq.size == 0)
      None
    else
    // sort by record number and pick first
      Some(allResultsSeq.toSeq.sorted(Ordering[RecordNumber].on[(RecordNumber, _)](_._1)).head._2)
  }

  def partition(pred: (T) => Boolean) = {
    val builder1 = newDistBuilder
    val builder2 = newDistBuilder

    // partition
    foreach(v => (if (pred(v)) builder1 else builder2) += v)

    // fetch results
    (builder1.result(), builder2.result())
  }

  def collect[B, That](pf: PartialFunction[T, B])(implicit bf: CanBuildFrom[Repr, B, That]) = {
    val rb = bf.asInstanceOf[CanDistBuildFrom[Repr, B, That]](repr)
    foreach(el => if (pf.isDefinedAt(el)) rb += pf(el))
    rb.result()
  }

  def ++[B >: T, That](that: GenTraversableOnce[B])(implicit bf: CanBuildFrom[Repr, B, That]) = {
    val remoteBuilder = bf.asInstanceOf[CanDistBuildFrom[Repr, T, That]](repr)
    //TODO (VJ) make flatten accept the builder
    //    flatten(that.asInstanceOf[DistIterable[B]])
    throw new UnsupportedOperationException("Waiting for complete removal of DistDo!!!")
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
    res.size > 0
  }

  //TODO (VJ) cleanup
  def exists(pred: (T) => Boolean) = {
    var found = false
    val builder = DistIterableBuilder[Boolean]

    foreach(el => if (!found && pred(el)) {
      builder += true
      found = true
    })

    val res = builder.result()
    ExecutionPlan.execute(res)
    res.size > 0
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

  def minBy[B](f: (T) => B)(implicit cmp: Ordering[B]) = {
    val result = groupBySort((v: T, emitter: Emitter[T]) => {
      emitter.emit(v);
      1
    }).combine((it: Iterable[T]) => it.minBy(f)(cmp))

    ExecutionPlan.execute(result)

    result.seq.head._2
  }

  def maxBy[B](f: (T) => B)(implicit cmp: Ordering[B]) = {
    val result = groupBySort((v: T, emitter: Emitter[T]) => {
      emitter.emit(v);
      1
    }).combine((it: Iterable[T]) => it.maxBy(f)(cmp))

    ExecutionPlan.execute(result)

    result.seq.head._2
  }

  def max[A1 >: T](implicit ord: Ordering[A1]) = {
    val result = groupBySort((v: T, emitter: Emitter[T]) => {
      emitter.emit(v);
      1
    }).combine((it: Iterable[T]) => it.max(ord))

    ExecutionPlan.execute(result)

    result.toSeq.head._2
  }

  def min[A1 >: T](implicit ord: Ordering[A1]) = {
    val result = groupBySort((v: T, emitter: Emitter[T]) => {
      emitter.emit(v);
      1
    }).combine((it: Iterable[T]) => it.min(ord))

    ExecutionPlan.execute(result)

    result.seq.head._2
  }

  def product[A1 >: T](implicit num: Numeric[A1]) = {
    val result = groupBySort((v: T, emitter: Emitter[T]) => {
      emitter.emit(v);
      1
    }).combine((it: Iterable[T]) => it.product(num))

    ExecutionPlan.execute(result)

    result.seq.head._2
  }

  def sum[A1 >: T](implicit num: Numeric[A1]) = {
    val result = groupBySort((v: T, emitter: Emitter[T]) => {
      emitter.emit(v);
      1
    }).combine((it: Iterable[T]) => it.sum(num))

    ExecutionPlan.execute(result)

    result.seq.head._2
  }

  def copyToArray[B >: T](xs: Array[B]) = copyToArray(xs, 0)

  def copyToArray[B >: T](xs: Array[B], start: Int) = copyToArray(xs, start, xs.length - start)

  def copyToArray[B >: T](xs: Array[B], start: Int, len: Int) = seq.copyToArray(xs, start, len)

  def scanLeft[S, That](z: S)(op: (S, T) => S)(implicit bf: CanBuildFrom[Repr, S, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //seq.scanLeft(z)(op)(bf2seq(bf))

  def scanRight[S, That](z: S)(op: (T, S) => S)(implicit bf: CanBuildFrom[Repr, S, That]) = throw new UnsupportedOperationException("Not implemented yet!!!")

  //seq.scanRight(z)(op)(bf2seq(bf))

  // TODO (vj) optimize when partitioning is introduced
  // TODO (vj) for now use only shared collection.
  def drop(n: Int) = {
    val rb = newDistBuilder
    //    rb.uniquenessPreserved

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

  // Not Implemented Yet
  def groupBy[K](f: (T) => K) = throw new UnsupportedOperationException("Not implemented yet!!!")
}
