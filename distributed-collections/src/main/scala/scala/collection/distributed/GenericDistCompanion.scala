package scala.collection.distributed

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

/** A template class for companion objects of distributed collection classes.
 *
 *  @define Coll DistIterable
 *  @tparam CC   the type constructor representing the collection class
 *  @since 2.8
 */
trait GenericDistCompanion[+CC[X] <: DistIterable[X]] {

  def newRemoteBuilder[A]: RemoteBuilder[A, CC[A]]

  def newBuilder[A]: RemoteBuilder[A, CC[A]]

}