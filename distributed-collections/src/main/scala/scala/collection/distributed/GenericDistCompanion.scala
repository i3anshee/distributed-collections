package scala.collection.distributed

/**A template class for companion objects of distributed collection classes.
 *
 * @define Coll DistIterable
 * @tparam CC   the type constructor representing the collection class
 */
trait GenericDistCompanion[+CC[X] <: DistIterable[X]] {

  def newRemoteBuilder[A]: RemoteBuilder[A, CC[A]]

  def newBuilder[A]: RemoteBuilder[A, CC[A]]

}

trait GenericDistMapCompanion[+CC[P, Q] <: DistMap[P, Q]] {
  def newRemoteBuilder[P, Q]: RemoteBuilder[(P, Q), CC[P, Q]]
}