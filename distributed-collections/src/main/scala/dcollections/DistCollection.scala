package dcollections

import api.dag.{CombinePlanNode, FlattenPlanNode, ParallelDoPlanNode, GroupByPlanNode}
import api.{DistContext, CollectionId, Emitter}
import builder.{DistCollectionBuilderFactory, DistCanBuildFrom}
import java.net.URI
import execution.{DCUtil, ExecutionPlan}
import mrapi.FSAdapter
import io.CollectionsIO

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object DistCollection {
  implicit def bf[A]: DistCanBuildFrom[DistCollection[A], A, DistCollection[A]] = new DistCollectionBuilderFactory[A]
}

/**
 * Super class for all distributed collections. Provides four basic primitives from which all other methods are built.
 */
class DistCollection[A](location: URI) extends CollectionId(location) {

  def parallelDo[B](parOperation: (A, Emitter[B], DistContext) => Unit): DistCollection[B] = {
    // add a parallel do node
    val outDistCollection = new DistCollection[B](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new ParallelDoPlanNode(outDistCollection, parOperation))
    ExecutionPlan.sendToOutput(node, outDistCollection)
    ExecutionPlan.execute()
    outDistCollection
  }

  def parallelDo[B](parOperation: (A, Emitter[B]) => Unit): DistCollection[B] = {
    parallelDo((a: A, emitter: Emitter[B], context: DistContext) => parOperation(a, emitter))
  }

  def groupBy[K, B](keyFunction: (A, Emitter[B]) => K): DistMap[K, Iterable[B]] = {
    // add a groupBy node to execution plan
    val outDistCollection = new DistMap[K, Iterable[B]](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new GroupByPlanNode[A, B, K](outDistCollection, keyFunction))
    ExecutionPlan.sendToOutput(node, outDistCollection)
    ExecutionPlan.execute()
    outDistCollection
  }

  /**Partitions this $coll into a map of ${coll}s according to some discriminator function.
   *
   *  Note: this method is not re-implemented by views. This means
   *        when applied to a view it will always force the view and
   *        return a new $coll.
   *
   * @param f     the discriminator function.
   * @tparam K    the type of keys returned by the discriminator function.
   * @return A map from keys to ${coll}s such that the following invariant holds:
   */
  def groupBy[K](keyFunction: A => K): DistMap[K, Iterable[A]] =
    groupBy((el, emitter) => {
      emitter.emit(el)
      keyFunction(el)
    })

  def flatten[B >: A](collections: Traversable[DistCollection[B]]): DistCollection[A] = {
    val outDistCollection = new DistCollection[A](DCUtil.generateNewCollectionURI)
    val node = ExecutionPlan.addFlattenNode(
      new FlattenPlanNode(outDistCollection, List(this) ++ collections)
    )
    ExecutionPlan.sendToOutput(node, outDistCollection)
    ExecutionPlan.execute()
    outDistCollection
  }

  def combineValues[K, B, C](keyFunction: (A, Emitter[B]) => K, op: (C, B) => C): DistCollection[Pair[K, C]] = {
    // add combine node
    val outDistCollection = new DistCollection[Pair[K, C]](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new CombinePlanNode[A, K, B, C](outDistCollection, keyFunction, op))
    ExecutionPlan.sendToOutput(node, outDistCollection)

    ExecutionPlan.execute()
    outDistCollection
  }

  /**Concatenates this $coll with the elements of a distributed collection.
   */
  def ++[B >: A](that: DistCollection[B]): DistCollection[A] = flatten(List(that))


  /**Builds a new collection by applying a function to all elements of this $coll.
   */
  // TODO (VJ) Resolve the issue with implicit conversions ( convert to the interface DistIterable? )
  def map[B, That](f: A => B)(implicit bf: DistCanBuildFrom[DistCollection[A], B, That]): That = {
    val builder = bf(this)
    builder += parallelDo((el, emitter) => emitter.emit(f(el)))
    builder.result
  }

  /**Applies a binary operator to a start value and all elements of this $coll, going left to right.
   */
  def fold[B >: A](z: B)(op: (B, B) => B): B = if (!isEmpty) op(reduce(op), z) else z

  /**Applies a binary operator to all elements of this $coll, going right to left.
   *  $willNotTerminateInf
   *  $orderDependentFold
   * @throws `UnsupportedOperationException` if this $coll is empty.
   */
  def reduce[B >: A](op: (B, B) => B): B = if (isEmpty)
    throw new UnsupportedOperationException("empty.reduce")
  else
    combineValues((v: A, emitter: Emitter[A]) => {
      emitter.emit(v);
      1
    }, op).asTraversable().head._2


  /**Builds a new collection by applying a function to all elements of this $coll
   *  and concatenating the results.
   */
  def flatMap[B](f: A => Traversable[B]): DistCollection[B] = parallelDo((el, emitter) => f(el).foreach((v) => emitter.emit(v)))

  /**Selects all elements of this $coll which satisfy a predicate.
   *
   * @param p     the predicate used to test elements.
   * @return a new $coll consisting of all elements of this $coll that satisfy the given
   *               predicate `p`. The order of the elements is preserved.
   */
  def filter(p: A => Boolean): DistCollection[A] = parallelDo((el, emitter) => if (p(el)) emitter.emit(el))

  /**Selects all elements of this $coll which do not satisfy a predicate.
   *
   * @param p     the predicate used to test elements.
   * @return a new $coll consisting of all elements of this $coll that do not satisfy the given
   *               predicate `p`. The order of the elements is preserved.
   */
  def filterNot(p: A => Boolean): DistCollection[A] = filter(!p(_))

  /**Builds a new collection by applying a partial function to all elements of this $coll
   *  on which the function is defined.
   */
  def collect[B](pf: PartialFunction[A, B]): DistCollection[B] = parallelDo((el, emitter) => if (pf.isDefinedAt(el)) emitter.emit(pf(el)))

  /**Builds a new collection by applying an option-valued function to all elements of this $coll
   *  on which the function is defined.
   */
  def filterMap[B](f: A => Option[B]): DistCollection[Option[B]] = parallelDo((el, emitter) => emitter.emit(f(el)))


  /**Partitions this $coll in two ${coll}s according to a predicate.
   *
   * @param p the predicate on which to partition.
   * @return a pair of ${coll}s: the first $coll consists of all elements that
   *           satisfy the predicate `p` and the second $coll consists of all elements
   *           that don't. The relative order of the elements in the resulting ${coll}s
   *           is the same as in the original $coll.
   */
  def partition(p: A => Boolean): (DistCollection[A], DistCollection[A]) = (
    parallelDo((el, emitter) => if (p(el)) emitter.emit(el)),
    parallelDo((el, emitter) => if (!p(el)) emitter.emit(el))
    )


  /**Applies option-valued function to successive elements of this $coll
   *  until a defined value is found.
   *
   *  $mayNotTerminateInf
   *  $orderDependent
   *
   * @param f    the function to be applied to successive elements.
   * @return an option value containing the first defined result of
   *              `f`, or `None` if `f` returns `None` for all all elements.
   */
  def mapFind[B](f: A => Option[B]): Option[B] = throw new UnsupportedOperationException("Waiting for global cache !!!")

  /**Tests whether a predicate holds for all elements of this $coll.
   *
   *  $mayNotTerminateInf
   *
   * @param p     the predicate used to test elements.
   * @return        `true` if the given predicate `p` holds for all elements
   *                 of this $coll, otherwise `false`.
   */
  def forall(p: A => Boolean): Boolean = throw new UnsupportedOperationException("Waiting for global cache !!!")

  /**Tests whether a predicate holds for some of the elements of this $coll.
   *
   *  $mayNotTerminateInf
   *
   * @param p     the predicate used to test elements.
   * @return        `true` if the given predicate `p` holds for some of the elements
   *                 of this $coll, otherwise `false`.
   */
  def exists(p: A => Boolean): Boolean = {
    var found = false
    parallelDo((el: A, em: Emitter[Boolean], context: DistContext) => if (found)
      if (p(el)) {
        em.emit(true)
        found = true
      }
    ).size > 0
  }


/**Finds the first element of the $coll satisfying a predicate, if any.
 *
 *  $orderDependent
 *
 * @param p    the predicate used to test elements.
 * @return an option value containing the first element in the $coll
 *              that satisfies `p`, or `None` if none exists.
 */
def find (p: A => Boolean): Option[A] = throw new UnsupportedOperationException ("Waiting for global cache !!!")

/**Selects the first element of this $coll.
 *  $orderDependent
 * @return the first element of this $coll.
 * @throws `NoSuchElementException` if the $coll is empty.
 */
def head: A = throw new UnsupportedOperationException ("Waiting for metadata!!!")

/**Optionally selects the first element.
 *  $orderDependent
 * @return the first element of this $coll if it is nonempty, `None` if it is empty.
 */
def headOption: Option[A] = if (isEmpty) None else Some (head)

/**Selects all elements except the first.
 *  $orderDependent
 * @return a $coll consisting of all elements of this $coll
 *           except the first one.
 * @throws `UnsupportedOperationException` if the $coll is empty.
 */
def tail: DistCollection[A] = {
if (isEmpty) throw new UnsupportedOperationException ("empty.tail")
drop (1)
}

/**Selects the last element.
 *  $orderDependent
 * @return the first element of this $coll.
 * @throws `NoSuchElementException` if the $coll is empty.
 */
def last: A = throw new UnsupportedOperationException ("Waiting for metadata!!!")

/**Optionally selects the last element.
 *  $orderDependent
 * @return the last element of this $coll$ if it is nonempty, `None` if it is empty.
 */
def lastOption: Option[A] = if (isEmpty) None else Some (last)

/**Selects all elements except the last.
 *  $orderDependent
 * @return a $coll consisting of all elements of this $coll
 *           except the last one.
 * @throws `UnsupportedOperationException` if the $coll is empty.
 */
def init: DistCollection[A] = {
if (isEmpty) throw new UnsupportedOperationException ("empty.init")
throw new UnsupportedOperationException ("Waiting for ordering and global cache !!!")
}

/**Selects first ''n'' elements.
 *  $orderDependent
 * @param n    Tt number of elements to take from this $coll.
 * @return a $coll consisting only of the first `n` elements of this $coll, or else the
 *          whole $coll, if it has less than `n` elements.
 */
def take (n: Int): DistCollection[A] = throw new UnsupportedOperationException ("Waiting for ordering and global cache !!!")

/**Selects all elements except first ''n'' ones.
 *  $orderDependent
 * @param n    the number of elements to drop from this $coll.
 * @return a $coll consisting of all elements of this $coll except the first `n` ones, or else the
 *          empty $coll, if this $coll has less than `n` elements.
 */
def drop (n: Int): DistCollection[A] = throw new UnsupportedOperationException ("Waiting for ordering and global cache !!!")

/**Selects an interval of elements.
 *
 *  Note: `c.slice(from, to)`  is equivalent to (but possibly more efficient than)
 *  `c.drop(from).take(to - from)`
 *  $orderDependent
 *
 * @param from   the index of the first returned element in this $coll.
 * @param until  the index one past the last returned element in this $coll.
 * @return a $coll containing the elements starting at index `from`
 *           and extending up to (but not including) index `until` of this $coll.
 */
def slice (from: Int, until: Int): DistCollection[A] = throw new UnsupportedOperationException ("Waiting for ordering and global cache !!!")

/**Takes longest prefix of elements that satisfy a predicate.
 *  $orderDependent
 * @param p  The predicate used to test elements.
 * @return the longest prefix of this $coll whose elements all satisfy
 *           the predicate `p`.
 */
def takeWhile (p: A => Boolean): DistCollection[A] = throw new UnsupportedOperationException ("Waiting for ordering and global cache !!!")

/**Drops longest prefix of elements that satisfy a predicate.
 *  $orderDependent
 * @param p  The predicate used to test elements.
 * @return the longest suffix of this $coll whose first element
 *           does not satisfy the predicate `p`.
 */
def dropWhile (p: A => Boolean): DistCollection[A] = throw new UnsupportedOperationException ("Waiting for ordering and global cache !!!")


/**Splits this $coll into two at a given position.
 *  Note: `c splitAt n` is equivalent to (but possibly more efficient than)
 *         `(c take n, c drop n)`.
 *  $orderDependent
 *
 * @param n the position at which to split.
 * @return a pair of ${coll}s consisting of the first `n`
 *           elements of this $coll, and the other elements.
 */
def splitAt (n: Int): (DistCollection[A], DistCollection[A] ) = throw new UnsupportedOperationException ("Waiting for ordering and global cache !!!")

/**Splits this $coll into a prefix/suffix pair according to a predicate.
 *
 *  Note: `c span p`  is equivalent to (but possibly more efficient than)
 *  `(c takeWhile p, c dropWhile p)`, provided the evaluation of the predicate `p`
 *  does not cause any side-effects.
 *  $orderDependent
 *
 * @param p the test predicate
 * @return a pair consisting of the longest prefix of this $coll whose
 *           elements all satisfy `p`, and the rest of this $coll.
 */
def span (p: A => Boolean): (DistCollection[A], DistCollection[A] ) = throw new UnsupportedOperationException ("Waiting for ordering and global cache !!!")

/**Partitions elements in fixed size ${coll}s.
 * @see Iterator#grouped
 *
 * @param size the number of elements per group
 * @return An iterator producing ${coll}s of size `size`, except the
 *          last will be truncated if the elements don't divide evenly.
 */
def grouped (size: Int): DistCollection[Traversable[A]] = throw new UnsupportedOperationException ("Waiting for local cache !!!")

/**Groups elements in fixed size blocks by passing a "sliding window"
 *  over them (as opposed to partitioning them, as is done in grouped.)
 *
 * @return An iterator producing ${coll}s of size `size`, except the
 *          last will be truncated if the elements don't divide evenly.
 */
def sliding (size: Int): DistCollection[Traversable[A]] = throw new UnsupportedOperationException ("Waiting for global cache!!!")

/**Groups elements in fixed size blocks by passing a "sliding window"
 *  over them (as opposed to partitioning them, as is done in grouped.)
 *
 * @return An iterator producing ${coll}s of size `size`, except the
 *          last will be truncated if the elements don't divide evenly.
 */
def sliding (size: Int, step: Int): DistCollection[Traversable[A]] = throw new UnsupportedOperationException ("Waiting for global cache!!!")

/**Selects last ''n'' elements.
 *  $orderDependent
 *
 * @param n the number of elements to take
 * @return a $coll consisting only of the last `n` elements of this $coll, or else the
 *          whole $coll, if it has less than `n` elements.
 */
def takeRight (n: Int): DistCollection[A] =
throw new UnsupportedOperationException ("Ordered collections !!!")

/**Selects all elements except last ''n'' ones.
 *  $orderDependent
 *
 * @param n    The number of elements to take
 * @return a $coll consisting of all elements of this $coll except the first `n` ones, or else the
 *          empty $coll, if this $coll has less than `n` elements.
 */
def dropRight (n: Int): DistCollection[A] =
throw new UnsupportedOperationException ("Ordered collections !!!")

/**Returns a $coll formed from this $coll and another iterable collection
 *  by combining corresponding elements in pairs.
 *  If one of the two collections is longer than the other, its remaining elements are ignored.
 *
 *  $orderDependent
 *
 * @return a new $coll containing pairs consisting of
 *                 corresponding elements of this $coll and `that`. The length
 *                 of the returned collection is the minimum of the lengths of this $coll and `that`.
 */
def zip[A1 >: A, B]: DistCollection[A] =
throw new UnsupportedOperationException ("Ordered collections !!!")

/**Returns a $coll formed from this $coll and another distributed collection
 *  by combining corresponding elements in pairs.
 *  If one of the two collections is shorter than the other,
 *  placeholder elements are used to extend the shorter collection to the length of the longer.
 */
def zipAll[B, A1 >: A] (that: DistCollection[B], thisElem: A1, thatElem: B): DistCollection[(A, B)] =
throw new UnsupportedOperationException ("Ordered collections !!!")


/**Zips this $coll with its indices.
 *
 *  $orderDependent
 */
def zipWithIndex[A1 >: A] (): DistCollection[A] =
throw new UnsupportedOperationException ("Ordered collections !!!")

/**Checks if the other distributed collection contains the same elements in the same order as this $coll.
 *
 * @param that  the collection to compare with.
 * @return `true`, if both collections contain the same elements in the same order, `false` otherwise.
 */
def sameElements[B >: A] (that: DistCollection[B] ): Boolean = throw new UnsupportedOperationException ("Waiting for ordered !!!")


def isEmpty: Boolean = (size == 0)

def size: Long = CollectionsIO.getCollectionMetaData (this).size

// NOT IMPLEMENTED
// scanLeft
// scanRight

def asTraversable (): Traversable[A] = FSAdapter.valuesTraversable[A] (location)

override def toString (): String = {
val builder = new StringBuilder ("[ ")
FSAdapter.valuesTraversable[A] (location).foreach ((v: A) => builder.append (v).append (" ") )
builder.append ("]")
builder.toString
}
}