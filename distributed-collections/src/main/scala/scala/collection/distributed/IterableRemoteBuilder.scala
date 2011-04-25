package scala.collection.distributed

/**
 * User: vjovanovic
 * Date: 4/25/11
 */

class IterableRemoteBuilder[Elem] extends RemoteBuilder[Elem, DistIterable[Elem]] {

  def result(collection: DistIterable[Elem]) = collection

  def uniquenessPreserved = {}

  def result() = null

  def clear() = null

  def +=(elem: Elem) = null
}


class DistCollRemoteBuilder[Elem] extends RemoteBuilder[Elem, DistColl[Elem]] {

  def result(collection: DistIterable[Elem]) = new DistColl(collection.location)

  def uniquenessPreserved = {}

  def result() = null

  def clear() = null

  def +=(elem: Elem) = null
}