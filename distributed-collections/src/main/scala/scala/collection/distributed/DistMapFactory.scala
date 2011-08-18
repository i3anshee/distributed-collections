package scala.collection.distributed

import api.shared.DistBuilderLike
import collection.mutable.Builder
import collection.generic.{GenMapFactory, CanCombineFrom, MapFactory}

/**
 * User: vjovanovic
 * Date: 4/26/11
 */

abstract class DistMapFactory[CC[X, Y] <: DistMap[X, Y] with DistMapLike[X, Y, CC[X, Y], _]]
  extends GenMapFactory[CC]
  with GenericDistMapCompanion[CC] {

  type MapColl = CC[_, _]

  /**The default builder for $Coll objects.
   * @tparam K      the type of the keys
   * @tparam V      the type of the associated values
   */
  override def newBuilder[K, V]: Builder[(K, V), CC[K, V]] = newDistBuilder[K, V]

  /**The default combiner for $Coll objects.
   * @tparam K     the type of the keys
   * @tparam V     the type of the associated values
   */
  def newDistBuilder[K, V]: DistBuilderLike[(K, V), CC[K, V]]

  class CanDistBuildFromMap[K, V] extends CanDistBuildFrom[CC[_, _], (K, V), CC[K, V]] {
    def apply(from: MapColl) = from.genericMapDistBuilder[K, V].asInstanceOf[DistBuilderLike[(K, V), CC[K, V]]]

    def apply() = newDistBuilder[K, V]
  }

}