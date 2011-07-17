package scala.collection.distributed

import api.shared.DistBuilderLike
import collection.GenMap
import shared.DistMapBuilder

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

  def newDistBuilder[K, V]: DistBuilderLike[(K, V), DistMap[K, V]] = DistMapBuilder[K, V]()

  implicit def canBuildFrom[K, V]: CanDistBuildFrom[Coll, (K, V), DistMap[K, V]] = new CanDistBuildFromMap[K, V]

}