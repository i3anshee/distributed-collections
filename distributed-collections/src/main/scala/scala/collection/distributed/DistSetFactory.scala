package scala.collection.distributed

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

import api.shared.DistBuilderLike
import collection.generic.SetFactory


abstract class DistSetFactory[CC[X] <: DistSet[X] with DistSetLike[X, CC[X], _] with GenericDistTemplate[X, CC]]
  extends SetFactory[CC]
  with GenericDistCompanion[CC] {

  def newDistBuilder[A]: DistBuilderLike[A, CC[A]]

  def newBuilder[A]: DistBuilderLike[A, CC[A]] = newDistBuilder[A]

  class GenericDistBuildFrom[A] extends CanDistBuildFrom[CC[_], A, CC[A]] {
    override def apply(from: Coll) = from.genericDistBuilder[A]

    override def apply() = newDistBuilder[A]
  }

}