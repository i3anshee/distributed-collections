package examples

import scala.collection.distributed.DistSet
import java.net.URI
import collection.mutable.ArrayBuffer

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

object SetExample {
  def main(args: Array[String]) {
    val ds1 = new DistSet[Long](new URI("./longsTo1k"))
    val ds2 = new DistSet[Long](new DistSet[Long](new URI("./longsTo1k")).filter(_ > 50).location)
    val ds2x1024 = new DistSet[Long](new DistSet[Long](new URI("./longsTo1k")).flatMap((v: Long) => for (i <- 0 to 2) yield v * 2L +  i).location)
    val messages = ArrayBuffer[String]()

    messages += "Intersection of (0 to 1024) and (50 to 1024) =" + (ds1 intersect ds2 toString())
    messages += "Diff of (0 to 1024) and (50 to 1024) =" + (ds1 diff ds2 toString())
    messages += "(0 to 1024) + 12345 contains 12345 =" + (ds1 + 12345 contains 12345)
    messages += "(0 to 1024) + 12345 contains 12346 =" + (ds1 + 12345 contains 12346)
    messages += "(0 to 1024) union (0 to 2 * 1024) =" + (ds1 union ds2x1024 toString())


    messages.foreach(println)
  }
}