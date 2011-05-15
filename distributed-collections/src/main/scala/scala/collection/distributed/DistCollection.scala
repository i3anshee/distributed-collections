package scala.collection.distributed

import java.net.URI

class DistCollection[T](uri: URI)
  extends DistIterable[T]
  with GenericDistTemplate[T, DistCollection] {
  self =>

  override def companion = DistCollection

  override def location = uri
}

object DistCollection extends DistFactory[DistCollection] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistCollection[T]] = new GenericCanDistBuildFrom[T]

  def newRemoteBuilder[T] = new DistCollRemoteBuilder[T]

  def newBuilder[T] = new DistCollRemoteBuilder[T]
}