package scala.collection.distributed

import api.Emitter

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

class DistSetRemoteBuilder[Elem] extends RemoteBuilder[Elem, DistSet[Elem]] {
  var elementsUnique = false

  /**
   * If uniqueness of elements is not preserved applies additional operations on the set.
   */
  def result(collection: DistIterable[Elem]) = {
    if (elementsUnique)
      new DistHashSet(collection.location)
    else
      new DistHashSet(
        collection.groupBy(_.hashCode)
          .parallelDo((pair: (Int, scala.Traversable[Elem]), emitter: Emitter[Elem]) => {
          val existing = scala.collection.mutable.HashSet[Elem]()
          pair._2.foreach((el) =>
            if (!existing.contains(el)) {
              existing += el
              emitter.emit(el)
            }
          )
        }).location
      )
  }

  def uniquenessPreserved = {
    elementsUnique = true
  }

  def result() = null

  def clear() = null

  def +=(elem: Elem) = null
}