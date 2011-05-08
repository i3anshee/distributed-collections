package examples

import java.net.URI
import collection.distributed.api.Emitter
import collection.immutable.{GenIterable, GenTraversable}
import collection.distributed.{DistHashMap, DistMap, DistColl}
import execution.ExecutionPlan

/**
 * @author Vojin Jovanovic
 */

object ExPlanDAGTest {
  def main(args: Array[String]) {
    val ds1 = new DistColl[Long](new URI("./longsTo1k"))
    val ds2 = new DistColl[Int](new URI("./intsTo1k"))
    val ds3 = new DistHashMap[Long, GenIterable[Int]](new URI("./stringsTo1k"))

    val map = ds1.filter(_ > 0).flatten(ds2)
//      .sgbr(key = (l: Long, em: Emitter[Int]) => {
//      em.emit(l.toInt); l
//    })
    ExecutionPlan.toString
    //    map.flatten(ds3)
  }
}