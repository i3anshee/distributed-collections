package util

import scala.colleciton.distributed.hadoop.FSAdapter
import java.net.URI
import scala.util.Random
import collection.mutable.ArrayBuffer

/**
 * User: vjovanovic
 * Date: 4/13/11
 */

object CreateCollections {

  def createInts1m = {
    FSAdapter.createDistCollection((0 to (1 * 1024 * 1024)), URI.create("./intsTo1m"))
    println("Created intsTo1m")
  }

  def createInts1k = {
    FSAdapter.createDistCollection((0 to (1 * 1024)), URI.create("./intsTo1k"))
    println("Created intsTo1k")
  }

  def createLongs1m = {
    FSAdapter.createDistCollection((0L to (1L * 1024L * 1024L)), URI.create("./longsTo1m"))
    println("Created longsTo1m")
  }

  def createStrings1m = {
    FSAdapter.createDistCollection((0 to (1 * 1024 * 1024)).map(_.toString), URI.create("./stringsTo1m"))
    println("Created stringsTo1m")
  }

  def createStrings1k = {
    FSAdapter.createDistCollection((0 to (1 * 1024)).map(_.toString), URI.create("./stringsTo1k"))
    println("Created stringsTo1k")
  }

  def createLongs1k = {
    FSAdapter.createDistCollection((0L to (1L * 1024L)), URI.create("./longsTo1k"))
    println("Created longsTo1k")
  }

  def createWordCountText100m = {
    FSAdapter.createDistCollection(new RandomTextIterable(100 * 1000 * 1000), URI.create("./textTo100m"))
    println("Created textTo100m")
  }

  def createWordCountText10m = {
    FSAdapter.createDistCollection(new RandomTextIterable(10 * 1000 * 1000), URI.create("./textTo10m"))
    println("Created textTo10m")
  }

  def createWordCountText1m = {
    FSAdapter.createDistCollection(new RandomTextIterable(1 * 1000 * 1000), URI.create("./textTo1m"))
    println("Created textTo1m")
  }

  def main(args: Array[String]) {
    createWordCountText1m
    createWordCountText10m
//    createWordCountText100m

  }

  class RandomTextIterable(val number: Int) extends Iterable[String] {

    def iterator = new Iterator[String]() {
      var i = 0

      def next() = {
        (i += 1)
        (0 to (5)).foldLeft("")((a, v) => a + Random.nextInt(1000000).toString + " ")
      }

      def hasNext = i < number
    }
  }


}