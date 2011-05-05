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

  //TODO (VJ) optimize the reading from dfs
  override def seq: HashSet[T] = HashSet.empty ++ super.seq

  override def empty: DistHashSet[T] = throw new UnsupportedOperationException("Not implemented yet!!")
}

object DistHashSet extends DistSetFactory[DistHashSet] {

  def newRemoteBuilder[T]: RemoteBuilder[T, DistHashSet[T]] = new DistHashSetRemoteBuilder[T]

  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistHashSet[T]] = new GenericDistBuildFrom[T]
}




