package scala.collection.distributed.builder

import scala.collection.distributed.DistCollection

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

/**The base trait of all distributed builders.
 *  A builder lets one construct a collection out of other distributed collections by enforcing constraints (for sets and maps)
 *
 * @tparam Elem  the type of elements that get added to the builder.
 * @tparam To    the type of collection that it produced.
 *
 */
trait DistBuilder[Elem, +To] {


  def +=(coll: DistCollection[Elem]): Unit

  /**Produces a collection from the added elements.
   *  The builder's contents are undefined after this operation.
   * @return a collection containing the elements added to this builder.
   */
  def result(): To
}