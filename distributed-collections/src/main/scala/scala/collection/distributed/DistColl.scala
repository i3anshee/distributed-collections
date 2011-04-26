package scala.collection.distributed

import java.net.URI

class DistColl[T](uri: URI)
  extends DistIterable[T]
  with GenericDistTemplate[T, DistColl]
{
  self =>

  override def companion = DistColl

  override def location = uri
}

object DistColl extends DistFactory[DistColl] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistColl[T]] = new GenericCanDistBuildFrom[T]

  def newRemoteBuilder[T] = new DistCollRemoteBuilder[T]

  def newBuilder[T] = new DistCollRemoteBuilder[T]
}