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

    val ds1 = new DistColl[Long](new URI("./longsTo1k"))
    val ds2 = new DistColl[Int](new URI("./intsTo1k"))

//    val map: DistMap[Long, GenIterable[Int]] = ds1.filter(_ > 0).flatten(ds2.map(_.toLong)).sgbr(key = (l: Long, em: Emitter[Int]) => {
//      em.emit(l.toInt); l
//    })

    val map: DistIterable[Long] = ds1.filter(_ > 50).flatten(ds2.map(_.toLong))

    ExecutionPlan.execute(map)
//    val ds1 = new DistColl[Long](new URI("./longsTo1k"))
//    val ds2 = new DistColl[Long](new DistColl[Long](new URI("./longsTo1k")).filter(_ > 50).location)
//    val messages = ArrayBuffer[String]()
//
//    messages += "(0 to 1024) map ((_ + 1).toString) =" + (ds1.map((v: Long) => v + 1).toString)
//
//    println("Ending DistCollTest example!!!")
//    messages.foreach(println)
    0
  }
}