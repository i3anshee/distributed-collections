package examples

import java.net.URI
import collection.mutable.ArrayBuffer
import collection.distributed._
import api.Emitter
import collection.immutable.GenIterable
import execution.ExecutionPlan

/**
 * User: vjovanovic
 * Date: 4/26/11
 */


object DistCollTest {

  def main(args: Array[String]) = {

    println("Starting DistCollTest example!!!")

    val ds1 = new DistCollection[Long](new URI("./longsTo1k"))
    val ds2 = new DistCollection[Int](new URI("./intsTo1k"))

    //    val map = ds1.filter(_ > 0).flatten(ds2.map(_.toLong)).groupBySort((l: Long, em: Emitter[Int]) => {
    //      em.emit(l.toInt); l
    //    }).filter(_._2.size > 100)
    //
    //    val b = ds1.filter(_ > 1000)
    val b = ds1.groupBySort((v: Long, emitter: Emitter[Long]) => {
      emitter.emit(v);
      1
    }).combine((it: Iterable[Long]) => it.reduce(_ + _)).filter(_._2 > 100).map(_._2)

    val c = ds1.flatten(ds2.map(_.toLong)).filter(_ < 10)

    val d = c.flatten(b)

    ExecutionPlan.execute(d)

    println(b.toString)
    println(c.toString)
    println(b.size)
    println(c.size)
    0
  }
}