package scala.collection.distributed

import java.net.URI
import shared.DistCollectionBuilder

class DistCollection[T](uri: URI)
  extends DistIterable[T]
  with GenericDistTemplate[T, DistCollection] {
  self =>

  override def companion = DistCollection

  override def location = uri

  def seq = remoteIterable

}

object DistCollection extends DistFactory[DistCollection] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistCollection[T]] = new GenericCanDistBuildFrom[T]

  def newDistBuilder[T] = DistCollectionBuilder[T]()

  def newBuilder[T] = DistCollectionBuilder[T]()
}