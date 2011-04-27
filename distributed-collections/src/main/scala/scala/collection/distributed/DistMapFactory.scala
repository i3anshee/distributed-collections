package scala.collection.distributed

import collection.mutable.Builder
import collection.generic.{CanCombineFrom, MapFactory}

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

abstract class DistMapFactory[CC[X, Y] <: DistMap[X, Y] with DistMapLike[X, Y, CC[X, Y], _]]
extends MapFactory[CC]
   with GenericDistMapCompanion[CC] {

  type MapColl = CC[_, _]

  /** The default builder for $Coll objects.
   *  @tparam K      the type of the keys
   *  @tparam V      the type of the associated values
   */
  override def newBuilder[K, V]: Builder[(K, V), CC[K, V]] = newRemoteBuilder[K, V]

  /** The default combiner for $Coll objects.
   *  @tparam K     the type of the keys
   *  @tparam V     the type of the associated values
   */
  def newRemoteBuilder[K, V]: RemoteBuilder[(K, V), CC[K, V]]

  class CanDistBuildFromMap[K, V] extends CanDistBuildFrom[CC[_, _], (K, V), CC[K, V]] {
    def apply(from: MapColl) = from.genericMapRemoteBuilder[K, V].asInstanceOf[RemoteBuilder[(K, V), CC[K, V]]]

    def apply() = newRemoteBuilder[K, V]
  }

}