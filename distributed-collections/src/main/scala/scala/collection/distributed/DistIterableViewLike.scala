package scala.collection.distributed

import collection.{IterableView, IterableViewLike, GenIterableView, GenIterableViewLike}

/**
 * @author Vojin Jovanovic
 */

protected[distributed] trait DistIterableViewLike[+T] extends DistIterable[T] {

}


//[+T,
//                          +Coll, // TODO (VJ) (<: DistIterable)
//                          +CollSeq,
//                          +This <: DistIterableView[T, Coll, CollSeq] with DistIterableViewLike[T, Coll, CollSeq, This, ThisSeq],
//                          +ThisSeq <: IterableView[T, CollSeq] with IterableViewLike[T, CollSeq, ThisSeq]]
//  extends GenIterableView[T, Coll]
//   with GenIterableViewLike[T, Coll, This]
//   with DistIterable[T]
//   with DistIterableLike[T, This, ThisSeq] {
//  def isView = true
//}
