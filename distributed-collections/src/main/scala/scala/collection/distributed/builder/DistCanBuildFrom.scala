package scala.collection.distributed.builder

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

/**A base trait for distributed builder factories.
 *
 * @tparam From  the type of the underlying collection that requests
 *                a builder to be created.
 * @tparam Elem  the element type of the collection to be created.
 * @tparam To    the type of the collection to be created.
 *
 * @see DistBuilder
 */
trait DistCanBuildFrom[-From, Elem, To] {

  /**Creates a new builder on request of a collection.
   * @param from  the collection requesting the builder to be created.
   * @return a builder for collections of type `To` with element type `Elem`.
   *          The collections framework usually arranges things so
   *          that the created builder will build the same kind of collection
   *          as `from`.
   */
  def apply(from: From): DistBuilder[Elem, To]

  /**Creates a new builder from scratch.
   *
   * @return a builder for collections of type `To` with element type `Elem`.
   * @see scala.collection.breakOut
   */
  def apply(): DistBuilder[Elem, To]
}