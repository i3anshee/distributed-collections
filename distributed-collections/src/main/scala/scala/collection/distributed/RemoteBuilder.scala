package scala.collection.distributed

import collection.mutable.Builder

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

trait RemoteBuilder[-Elem, +To] extends Builder[Elem, To] {

  def uniquenessPreserved

  def result(collection: DistIterable[Elem]): To

}