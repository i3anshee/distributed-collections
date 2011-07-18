package scala.collection.distributed

/**
 * @author Vojin Jovanovic
 */

import java.net.URI
import shared.DistCollectionViewBuilder

//TODO (VJ) finish all methods
class DistCollectionView[T](uri: URI)
  extends DistIterableView[T, DistCollection[T], Iterable[T]]
  with GenericDistTemplate[T, DistCollectionView] {
  self =>

  override def companion = DistCollectionView

  override def location = uri

  //TODO (VJ) I need an iterable view that triggers the distributed computation after being forced
  def seq = throw new UnsupportedOperationException("Need a lazy iterator here. Will be implemented later.")
  // FSAdapter.iterator with a special view that triggers distributed computation

  protected[this] def viewIdString = ""

  protected[this] def viewIdentifier = ""

  protected def underlying = new DistCollection[T](location)

  override protected[this] def newDistBuilder = DistCollectionViewBuilder[T]()

  def view = throw new UnsupportedOperationException("Not implemented yet!!! Or should it be?")
}

object DistCollectionView extends DistFactory[DistCollectionView] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistCollectionView[T]] = new GenericCanDistBuildFrom[T]

  def newDistBuilder[T] = DistCollectionViewBuilder[T]()

  def newBuilder[T] = DistCollectionViewBuilder[T]()
}