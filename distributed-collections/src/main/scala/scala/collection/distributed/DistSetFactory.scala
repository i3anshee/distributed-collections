package scala.collection.distributed

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

import collection.generic.SetFactory


abstract class DistSetFactory[CC[X] <: DistSet[X] with DistSetLike[X, CC[X], _] with GenericDistTemplate[X, CC]]
  extends SetFactory[CC]
  with GenericDistCompanion[CC] {

  def newRemoteBuilder[A]: RemoteBuilder[A, CC[A]]

  def newBuilder[A]: RemoteBuilder[A, CC[A]] = newRemoteBuilder[A]

  class GenericDistBuildFrom[A] extends CanDistBuildFrom[CC[_], A, CC[A]] {
    override def apply(from: Coll) = from.genericRemoteBuilder[A]

    override def apply() = newBuilder[A]
  }

}