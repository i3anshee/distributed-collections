package scala.collection.distributed

import api.CollectionId
import java.net.URI
import collection.immutable.HashSet
import collection.generic.{GenericCompanion}
import collection.GenSet

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

class DistHashSet[T](uri: URI) extends DistSet[T]
with GenericDistTemplate[T, DistHashSet]
with DistSetLike[T, DistHashSet[T], HashSet[T]]
with CollectionId
with Serializable {
  def location = uri


  override def companion: GenericCompanion[DistHashSet] with GenericDistCompanion[DistHashSet] = DistHashSet

  override def empty: DistHashSet[T] = throw new UnsupportedOperationException("Not implemented yet!!")

  override def seq = throw new UnsupportedOperationException("Not implemented yet!!")

  override def size = throw new UnsupportedOperationException("Not implemented yet!!")

  def diff(that: GenSet[T]) = throw new UnsupportedOperationException("Not implemented yet!!")

  def canEqual(that: Any) = throw new UnsupportedOperationException("Not implemented yet!!")
}

object DistHashSet extends DistSetFactory[DistHashSet] {

  def newRemoteBuilder[T]: RemoteBuilder[T, DistHashSet[T]] = new DistHashSetRemoteBuilder[T]

  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistHashSet[T]] = new GenericDistBuildFrom[T]
}




