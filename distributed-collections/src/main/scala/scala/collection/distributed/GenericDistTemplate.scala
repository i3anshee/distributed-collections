package scala.collection.distributed

import annotation.unchecked.uncheckedVariance
import collection.generic.{GenericCompanion, GenericTraversableTemplate}
import collection.mutable.Builder

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

trait GenericDistTemplate[+A, +CC[X] <: DistIterable[X]]
  extends GenericTraversableTemplate[A, CC]
  with HasNewRemoteBuilder[A, CC[A]@uncheckedVariance] {

  def companion: GenericCompanion[CC] with GenericDistCompanion[CC]

  protected[this] override def newBuilder: collection.mutable.Builder[A, CC[A]] = companion.newBuilder[A]

  override def genericBuilder[B]: Builder[B, CC[B]] = genericRemoteBuilder[B]

  protected[this] def newRemoteBuilder = genericRemoteBuilder

  def genericRemoteBuilder[B]: RemoteBuilder[B, CC[B]] = companion.newRemoteBuilder[B]

}

trait GenericDistMapTemplate[K, +V, +CC[X, Y] <: DistMap[X, Y]] extends GenericDistTemplate[(K, V), DistIterable]
{
  protected[this] override def newRemoteBuilder: RemoteBuilder[(K, V), CC[K, V]] = mapCompanion.newRemoteBuilder[K, V]

  def mapCompanion: GenericDistMapCompanion[CC]

  def genericMapRemoteBuilder[P, Q]: RemoteBuilder[(P, Q), CC[P, Q]] = mapCompanion.newRemoteBuilder[P, Q]
}