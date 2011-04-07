package dcollections

import api.Emitter
import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/13/11
 */

class DistSet[A](location: URI) extends DistCollection[A](location) {

  override def map[B](f: A => B): DistSet[B] = {
    val resultCollection = parallelDo((elem: A, emitter: Emitter[B]) => {
      emitter.emit(f(elem))
    }).groupBy(_.hashCode)
      .parallelDo((pair: (Int, scala.Traversable[B]), emitter: Emitter[B]) => {
      val existing = scala.collection.mutable.HashSet[B]()
      pair._2.foreach((el: B) =>
        if (!existing.contains(el)) {
          existing += el
          emitter.emit(el)
        }
      )

    })

    new DistSet[B](resultCollection.location)
  }

}
