package util

import mrapi.FSAdapter
import java.net.URI

/**
 * User: vjovanovic
 * Date: 4/13/11
 */

object CreateCollections {
  def main(args: Array[String]) {

    FSAdapter.createDistCollection((0L to (1L * 1024L * 1024L)), URI.create("./longsTo1m"))
    println("Created longsTo1m")
    FSAdapter.createDistCollection((0 to (1 * 1024 * 1024)), URI.create("./intsTo1m"))
    println("Created intsTo1m")
    FSAdapter.createDistCollection((0 to (1 * 1024 * 1024)).map(_.toString), URI.create("./stringsTo1m"))
    println("Created stringsTo1m")

    FSAdapter.createDistCollection((0L to (1L * 1024L)), URI.create("./longsTo1k"))
    println("Created longsTo1k")
    FSAdapter.createDistCollection((0 to (1 * 1024)), URI.create("./intsTo1k"))
    println("Created intsTo1k")
    FSAdapter.createDistCollection((0 to (1 * 1024)).map(_.toString), URI.create("./stringsTo1k"))
    println("Created stringsTo1k")
  }
}