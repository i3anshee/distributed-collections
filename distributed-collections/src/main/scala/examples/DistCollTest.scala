package examples

import java.net.URI
import collection.mutable.ArrayBuffer
import collection.distributed._

/**
 * User: vjovanovic
 * Date: 4/26/11
 */


object DistCollTest {

  def main(args: Array[String]) = {

    println("Starting DistCollTest example!!!")
    val ds1 = new DistColl[Long](new URI("./longsTo1k"))
    val ds2 = new DistColl[Long](new DistColl[Long](new URI("./longsTo1k")).filter(_ > 50).location)
    val messages = ArrayBuffer[String]()

    messages += "(0 to 1024) map ((_ + 1).toString) =" + (ds1.map((v: Long) => v + 1).toString)

    println("Ending DistCollTest example!!!")
    messages.foreach(println)
    0
  }
}