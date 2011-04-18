package scala.collection.distributed.builder

import scala.collection.distributed.{DistCollection}

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

class DistCollectionBuilder[Elem] extends DistBuilder[Elem, DistCollection[Elem]] {
  var myColl: DistCollection[Elem] = null

  def +=(coll: DistCollection[Elem]) = myColl = coll

  def result() = myColl
}

