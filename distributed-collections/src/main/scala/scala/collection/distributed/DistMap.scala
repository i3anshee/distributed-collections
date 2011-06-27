package scala.collection.distributed

import collection.GenMap

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

trait DistMap[K, +V]
  extends GenMap[K, V]
  with GenericDistMapTemplate[K, V, DistMap]
  with DistIterable[(K, V)]
  with DistMapLike[K, V, DistMap[K, V], Map[K, V]] {


def mapCompanion: GenericDistMapCompanion[DistMap] = DistMap

def empty: DistMap[K, V] = throw new UnsupportedOperationException ("Not implemented yet!!!")

override def stringPrefix = "DistMap"
}

object DistMap extends DistMapFactory[DistMap] {
  def empty[K, V]: DistMap[K, V] = throw new UnsupportedOperationException("Not implemented yet!!!")

  def newRemoteBuilder[K, V]: RemoteBuilder[(K, V), DistMap[K, V]] = new DistMapRemoteBuilder[K, V]

  implicit def canBuildFrom[K, V]: CanDistBuildFrom[Coll, (K, V), DistMap[K, V]] = new CanDistBuildFromMap[K, V]

}