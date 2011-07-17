package scala.collection.distributed

import api.shared.DistBuilderLike
import collection.generic.CanBuildFrom

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

trait CanDistBuildFrom[-From, -Elem, +To] extends CanBuildFrom[From, Elem, To] {
  def apply(from: From): DistBuilderLike[Elem, To]
  def apply(): DistBuilderLike[Elem, To]
}