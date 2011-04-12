package examples

import dcollections.DistSet
import java.net.URI
import collection.mutable.ArrayBuffer

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

object SetExample {
  def main(args: Array[String]) {
    val distributedSet = new DistSet[Long](new URI("long-set"))
    val ds2 = new DistSet[Long](new DistSet[Long](new URI("long-set1")).filter(_ > 50).location)
    val ds2000 = new DistSet[Long](new DistSet[Long](new URI("long-set1")).flatMap((v: Long) => for (i <- 0 to 20) yield (v + i) * 20L).location)
    val messages = ArrayBuffer[String]()

    messages += "Intersection of (0 to 99) and (50 to 99) =" + (distributedSet intersect ds2 toString())
    messages += "Diff of (0 to 99) and (50 to 99) =" + (distributedSet diff ds2 toString())
    messages += "(0 to 99) + 12345 contains 12345 =" + (distributedSet + 12345 contains 12345)
    messages += "(0 to 99) + 12345 contains 12346 =" + (distributedSet + 12345 contains 12346)
    messages += "(0 to 99) union (0 to 2000) =" + (distributedSet union ds2000 toString())


    messages.foreach(println)
  }
}