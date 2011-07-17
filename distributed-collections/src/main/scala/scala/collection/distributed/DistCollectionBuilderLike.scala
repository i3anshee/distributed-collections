package scala.collection.distributed

import api.shared.DistBuilderLike

/**
 * @author Vojin Jovanovic
 */

trait DistCollectionBuilderLike[-Elem, +To] extends DistBuilderLike[Elem, To] {

  def applyConstraints(): Unit
}