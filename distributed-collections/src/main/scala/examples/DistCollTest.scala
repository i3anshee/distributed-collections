package examples

import java.net.URI
import collection.distributed._
import api.Emitter
import execution.ExecutionPlan
import shared.DistCounter
import collection.parallel.{ParSet, ParSeq}
import collection.generic.CanBuildFrom

/**
 * User: vjovanovic
 * Date: 4/26/11
 */


object DistCollTest {

  val examples: Map[String, (Array[String]) => Int] = Map(
    ("Demo" -> demo)
  )

  def demo(args: Array[String]) = {
    val longCol = new DistCollection[Long](new URI("./kryo-longsTo1m"))
    val longSet = new DistHashSet[Long](new URI("./kryo-longsTo1m"))

    val materialized1 = longCol.view.filter(v => true).map(_ + 1).mark

    val materialized2 = (longCol.view.filter(v => true) ++ materialized1).force

    println(materialized1)
    println(materialized2)
    0
  }

  def main(args: Array[String]) = {
    examples(args(0))(args.tail.toArray)
    0
  }
}
