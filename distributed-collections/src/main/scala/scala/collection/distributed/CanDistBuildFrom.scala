package scala.collection.distributed

import collection.generic.CanBuildFrom

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

trait CanDistBuildFrom[-From, -Elem, +To] extends CanBuildFrom[From, Elem, To] {
  def apply(from: From): RemoteBuilder[Elem, To]
  def apply(): RemoteBuilder[Elem, To]
}