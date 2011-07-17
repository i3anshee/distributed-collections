package scala.collection.distributed

import api.CollectionId
import api.shared.DistBuilderLike
import java.net.URI
import collection.generic.GenericCompanion
import collection.immutable
import shared.{DistHashSetBuilder, DistSetBuilder}

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

class DistHashSet[T](uri: URI) extends DistSet[T]
with GenericDistTemplate[T, DistHashSet]
with DistSetLike[T, DistHashSet[T], immutable.Set[T]]
with CollectionId
with Serializable {

  def location = uri

  override def companion: GenericCompanion[DistHashSet] with GenericDistCompanion[DistHashSet] = DistHashSet

  override def seq = remoteIterable.toSet[T]

  override def empty: DistHashSet[T] = throw new UnsupportedOperationException("Not implemented yet!!")
}

object DistHashSet extends DistSetFactory[DistHashSet] {

  def newDistBuilder[T]: DistBuilderLike[T, DistHashSet[T]] = DistHashSetBuilder[T]()

  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistHashSet[T]] = new GenericDistBuildFrom[T]
}




