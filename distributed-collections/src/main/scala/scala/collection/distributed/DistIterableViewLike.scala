package scala.collection.distributed

import api.shared.DistBuilderLike
import collection._
import generic.CanBuildFrom
import shared.DistCollectionViewBuilder
import execution.ExecutionPlan

/**
 * @author Vojin Jovanovic
 */

protected[distributed] trait DistIterableViewLike
[+T,
+Coll,
+CollSeq,
+This <: DistIterableView[T, Coll, CollSeq] with DistIterableViewLike[T, Coll, CollSeq, This, ThisSeq],
+ThisSeq <: IterableView[T, CollSeq] with IterableViewLike[T, CollSeq, ThisSeq]]
  extends GenIterableView[T, Coll]
  with GenIterableViewLike[T, Coll, This]
  with GenericDistTemplate[T, DistIterable]
  with DistIterable[T]
  with DistIterableLike[T, This, ThisSeq] {

  override def isView = true

  //TODO (VJ) fix this issue in a sweeter way
  override protected[this] def newDistBuilder: DistBuilderLike[T, This] =
    DistCollectionViewBuilder[T]().asInstanceOf[DistBuilderLike[T, This]]

  //TODO (VJ) move to generic form (good for now...)
  def mark: Coll = {
    ExecutionPlan.markCollection(this)
    underlying
  }

  def force[B >: T, That](implicit bf: CanBuildFrom[Coll, B, That]) = {
    forceExecute
    val builder = bf.asInstanceOf[CanDistBuildFrom[Coll, B, That]](underlying)
    builder.result(location)
  }
}