package scala.collection.distributed

import api.shared.DistBuilderLike
import collection.generic.GenericCompanion
import collection.GenSet
import shared.DistSetBuilder

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

trait DistSet[T] extends GenSet[T]
with GenericDistTemplate[T, DistSet]
with DistIterable[T]
with DistSetLike[T, DistSet[T], Set[T]] {
  self =>
  override def empty: DistSet[T] = throw new UnsupportedOperationException("")

  override def companion: GenericCompanion[DistSet] with GenericDistCompanion[DistSet] = DistSet

  override def stringPrefix = "DistSet"

}

object DistSet extends DistSetFactory[DistSet] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistSet[T]] = new GenericDistBuildFrom[T]

  def newDistBuilder[T]: DistBuilderLike[T, DistSet[T]] = DistSetBuilder[T]()
}
