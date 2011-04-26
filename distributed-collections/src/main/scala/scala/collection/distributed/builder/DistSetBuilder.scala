package scala.collection.distributed.builder

import scala.collection.distributed.api.Emitter
import scala.collection.distributed.{DistSetOld, DistCollection}

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

class DistSetBuilder[Elem] extends DistBuilder[Elem, DistSetOld[Elem]] {
  var myColl: DistCollection[Elem] = null

  def +=(coll: DistCollection[Elem]) = (myColl = coll)

  def result() = {
    // TODO (VJ) add the check if the set constraint is needed (filter -> takeLeft does not require set constraint)
    val result = myColl.groupBy(_.hashCode)
      .parallelDo((pair: (Int, scala.Traversable[Elem]), emitter: Emitter[Elem]) => {
      val existing = scala.collection.mutable.HashSet[Elem]()
      pair._2.foreach((el) =>
        if (!existing.contains(el)) {
          existing += el
          emitter.emit(el)
        }
      )
    })
    new DistSetOld[Elem](result.location)
  }
}

