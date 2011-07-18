package scala.collection.distributed

import collection.{IterableView, GenIterableView}
import shared.DistCollectionViewBuilder

/**
 * @author Vojin Jovanovic
 */

trait DistIterableView[+T, +Coll, +CollSeq]
  extends DistIterableViewLike[T, Coll, CollSeq, DistIterableView[T, Coll, CollSeq], IterableView[T, CollSeq]]
  with GenIterableView[T, Coll] {

}

object DistIterableView {
  type Coll = DistIterableView[_, C, _] forSome {type C <: DistIterable[_]}

  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistIterableView[T, DistIterable[T], Iterable[T]]] =
    new CanDistBuildFrom[Coll, T, DistIterableView[T, DistIterable[T], Iterable[T]]] {
      //TODO (VJ) make a view polymorphic factory
      override def apply(from: Coll) = DistCollectionViewBuilder[T]()

      override def apply() = DistCollectionViewBuilder[T]()
    }

}