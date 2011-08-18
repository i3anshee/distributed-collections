package scala.collection.distributed

import collection.generic.{GenTraversableFactory, GenericParCompanion, TraversableFactory}

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

abstract class DistFactory[CC[X] <: DistIterable[X] with GenericDistTemplate[X, CC]]
  extends GenTraversableFactory[CC]
  with GenericDistCompanion[CC] {

  class GenericCanDistBuildFrom[A] extends GenericCanBuildFrom[A] with CanDistBuildFrom[CC[_], A, CC[A]] {
    override def apply(from: Coll) = from.genericDistBuilder

    override def apply() = newBuilder[A]
  }
}