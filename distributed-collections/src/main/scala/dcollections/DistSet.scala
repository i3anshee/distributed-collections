package dcollections

import api.Emitter
import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/13/11
 */

class DistSet[A](location: URI) extends DistCollection[A](location) {

  override def map[B](f: A => B): DistSet[B] = ensureSet(parallelDo((elem: A, emitter: Emitter[B]) => {
    emitter.emit(f(elem))
  }))

  /**Tests if some element is contained in this set.
   *
   * @param elem the element to test for membership.
   * @return     `true` if `elem` is contained in this set, `false` otherwise.
   */
  def contains(elem: A): Boolean = exists((p) => p == elem)

  /**Creates a new set with an additional element, unless the element is
   *  already present.
   *
   * @param elem the element to be added
   * @return a new set that contains all elements of this set and that also
   *          contains `elem`.
   */
  def +(elem: A): DistSet[A] = throw new UnsupportedOperationException("Unsupported operation!!!")

  /**Creates a new set with a given element removed from this set.
   *
   * @param elem the element to be removed
   * @return a new set that contains all elements of this set but that does not
   *          contain `elem`.
   */
  def -(elem: A): DistSet[A] = throw new UnsupportedOperationException("Unsupported operation!!!")

  /**Tests if some element is contained in this set.
   *
   *  This method is equivalent to `contains`. It allows sets to be interpreted as predicates.
   * @param elem the element to test for membership.
   * @return  `true` if `elem` is contained in this set, `false` otherwise.
   */
  def apply(elem: A): Boolean = contains(elem)

  /**Computes the intersection between this set and another set.
   *
   * @param that  the set to intersect with.
   * @return a new set consisting of all elements that are both in this
   *  set and in the given set `that`.
   */
  def intersect(that: DistSet[A]): DistSet[A] = ensureSet(filter(that.contains))

  /**Computes the intersection between this set and another set.
   *
   *  '''Note:'''  Same as `intersect`.
   * @param that  the set to intersect with.
   * @return a new set consisting of all elements that are both in this
   *  set and in the given set `that`.
   */
  def &(that: DistSet[A]): DistSet[A] = intersect(that)

  /**Computes the union between of set and another set.
   *
   * @param that  the set to form the union with.
   * @return a new set consisting of all elements that are in this
   *  set or in the given set `that`.
   */
  def union(that: DistSet[A]): DistSet[A] = ensureSet(this.++(that))

  /**Computes the union between this set and another set.
   *
   *  '''Note:'''  Same as `union`.
   * @param that  the set to form the union with.
   * @return a new set consisting of all elements that are in this
   *  set or in the given set `that`.
   */
  def |(that: DistSet[A]): DistSet[A] = union(that)

  /**Computes the difference of this set and another set.
   *
   * @param that the set of elements to exclude.
   * @return a set containing those elements of this
   *              set that are not also contained in the given set `that`.
   */
  //def diff(that: DistSet[A]): DistSet[A] = --(that)

  /**The difference of this set and another set.
   *
   *  '''Note:'''  Same as `diff`.
   * @param that the set of elements to exclude.
   * @return a set containing those elements of this
   *              set that are not also contained in the given set `that`.
   */
  //def &~(that: DistSet[A]): DistSet[A] = diff(that)

  /**Tests whether this set is a subset of another set.
   *
   * @param that  the set to test.
   * @return     `true` if this set is a subset of `that`, i.e. if
   *              every element of this set is also an element of `that`.
   */
  //def subsetOf(that: DistSet[A]): Boolean = throw new UnsupportedOperationException("Unsupported operation!!!")


  private def ensureSet[B](collection: DistCollection[B]): DistSet[B] = {
    val result = collection.groupBy(_.hashCode)
      .parallelDo((pair: (Int, scala.Traversable[B]), emitter: Emitter[B]) => {
      val existing = scala.collection.mutable.HashSet[B]()
      pair._2.foreach((el) =>
        if (!existing.contains(el)) {
          existing += el
          emitter.emit(el)
        }
      )
    })
    new DistSet[B](result.location)
  }
}
