package scala.collection.distributed

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

trait HasNewRemoteBuilder[+A, +Repr] {
  /** The builder that builds instances of Repr */
  protected[this] def newRemoteBuilder: RemoteBuilder[A, Repr]
}