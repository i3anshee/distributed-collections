package scala.collection.distributed

import api.shared.DistBuilderLike
import java.net.URI
import shared.DistCollectionBuilder

class DistCollection[T](uri: URI)
  extends DistIterable[T]
  with GenericDistTemplate[T, DistCollection] {
  self =>

  override def companion = DistCollection

  override def location = uri

  def seq = remoteIterable

  override def view = new DistCollectionView[T](location)
}

object DistCollection extends DistFactory[DistCollection] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistCollection[T]] = new GenericCanDistBuildFrom[T]

  def newDistBuilder[T]: DistBuilderLike[T, DistCollection[T]] = DistCollectionBuilder[T]()

  def newBuilder[T]: DistBuilderLike[T, DistCollection[T]] = DistCollectionBuilder[T]()
}