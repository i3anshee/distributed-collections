package scala.collection.distributed

import collection.generic.GenericCompanion
import collection.GenSet

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

  def newRemoteBuilder[T]: RemoteBuilder[T, DistSet[T]] = new DistSetRemoteBuilder[T]
}
