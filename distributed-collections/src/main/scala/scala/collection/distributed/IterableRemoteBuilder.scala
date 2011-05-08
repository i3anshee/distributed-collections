package scala.collection.distributed

import api.Emitter
import collection.immutable.GenTraversable

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
  def result(collection: DistIterable[Elem]): DistHashSet[Elem] = {
    if (elementsUnique)
      new DistHashSet[Elem](collection.location)
    else
      new DistHashSet[Elem](
        collection.groupBy(_.hashCode).distDo((pair: (Int, GenTraversable[Elem]), emitter: Emitter[Elem]) => {
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

class DistHashSetRemoteBuilder[Elem]
  extends DistSetRemoteBuilder[Elem]
  with RemoteBuilder[Elem, DistHashSet[Elem]] {

}

class DistMapRemoteBuilder[K, V] extends RemoteBuilder[(K, V), DistMap[K, V]] {
  var elementsUnique = false

  /**
   * If uniqueness of elements is not preserved applies additional operations on the set.
   */
  def result(collection: DistIterable[(K, V)]): DistHashMap[K, V] = {
//    if (elementsUnique)
      new DistHashMap[K, V](collection.location)
//    else
//      new DistHashMap[K, V](
//        collection.sgbr(key = (el: (K, V), em: Emitter[V]) => {
//          em.emit(el._2)
//          el._1
//        }).distDo((pair: (K, GenTraversable[V]), em: Emitter[(K, V)]) => em.emit((pair._1, pair._2.head))).location
//      )
  }

  def uniquenessPreserved = {
    elementsUnique = true
  }

  def result() = null

  def clear() = null

  def +=(elem: (K, V)) = null
}

class DistHashMapRemoteBuilder[K, V]
  extends DistMapRemoteBuilder[K, V]
  with RemoteBuilder[(K, V), DistHashMap[K, V]] {

}
