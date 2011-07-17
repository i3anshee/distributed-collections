package scala.collection.distributed

import api.shared.DistBuilderLike
import shared.DistIterableBuilder

/**A template class for companion objects of distributed collection classes.
 *
 * @define Coll DistIterable
 * @tparam CC   the type constructor representing the collection class
 */
trait GenericDistCompanion[+CC[X] <: DistIterable[X]] {

  def newDistBuilder[A]: DistBuilderLike[A, CC[A]]

  def newBuilder[A]: DistBuilderLike[A, CC[A]]

}

trait GenericDistMapCompanion[+CC[P, Q] <: DistMap[P, Q]] {
  def newDistBuilder[P, Q]: DistBuilderLike[(P, Q), CC[P, Q]]
}