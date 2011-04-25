package scala.collection.distributed

import api.CollectionId
import collection.GenIterable
import collection.generic.{CanCombineFrom, GenericCompanion}
import collection.parallel.Combiner

/**
 * User: vjovanovic
 * Date: 4/23/11
 */

trait DistIterable[+T]
  extends GenIterable[T]
  with GenericDistTemplate[T, DistIterable]
  with DistIterableLike[T, DistIterable[T], Iterable[T]]
  with CollectionId
{

  override def companion: GenericCompanion[DistIterable] with GenericDistCompanion[DistIterable] = DistIterable

  def stringPrefix = "DistIterable"

}

/**$factoryInfo
 */
object DistIterable extends DistFactory[DistIterable] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistIterable[T]] = new GenericCanDistBuildFrom[T]

  def newRemoteBuilder[T] = new IterableRemoteBuilder[T]

  def newBuilder[T] = new IterableRemoteBuilder[T]
}
