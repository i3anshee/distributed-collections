package scala.collection.distributed

import api.shared.DistBuilderLike

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

trait HasNewDistBuilder[+T, +Repr] {
  /** The builder that builds instances of Repr */
  protected[this] def newDistBuilder: DistBuilderLike[T, Repr]
}