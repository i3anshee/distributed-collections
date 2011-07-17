package scala.collection.distributed

import annotation.unchecked.uncheckedVariance
import api.shared.DistBuilderLike
import collection.generic.{GenericCompanion, GenericTraversableTemplate}
import collection.mutable.Builder

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

trait GenericDistTemplate[+A, +CC[X] <: DistIterable[X]]
  extends GenericTraversableTemplate[A, CC]
  with HasNewDistBuilder[A, CC[A]@uncheckedVariance] {

  def companion: GenericCompanion[CC] with GenericDistCompanion[CC]

  protected[this] override def newBuilder: collection.mutable.Builder[A, CC[A]] = companion.newBuilder[A]

  override def genericBuilder[B]: Builder[B, CC[B]] = genericDistBuilder[B]

  protected[this] def newDistBuilder = genericDistBuilder

  def genericDistBuilder[B]: DistBuilderLike[B, CC[B]] = companion.newDistBuilder[B]

}

trait GenericDistMapTemplate[K, +V, +CC[X, Y] <: DistMap[X, Y]] extends GenericDistTemplate[(K, V), DistIterable] {
  protected[this] override def newDistBuilder: DistBuilderLike[(K, V), CC[K, V]] = mapCompanion.newDistBuilder[K, V]

  def mapCompanion: GenericDistMapCompanion[CC]

  def genericMapDistBuilder[P, Q]: DistBuilderLike[(P, Q), CC[P, Q]] = mapCompanion.newDistBuilder[P, Q]
}