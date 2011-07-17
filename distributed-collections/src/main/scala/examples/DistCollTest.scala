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
    ("WordCountDebug" -> wordCountDebug),
    ("WordCount" -> wordCount),
    ("WordCountNative" -> wordCountNative),
    ("Demo" -> demo),
    ("WordCountVerbose" -> wordCountVerbose)
  )

  def wordCountDebug(args: Array[String]) = {
    val lines = new DistCollection[String](new URI(args(0)))

    val out = lines.flatMap(_.split("\\s").toTraversable).map(v => (v, 1)).groupByKey.combine(_.sum)

    ExecutionPlan.execute(out)
    0
  }

  def wordCount(args: Array[String]) = {
    val lines = new DistCollection[String](new URI(args(0)))

    val out = lines.flatMap(_.split("\\s").toTraversable).groupBySort((v: String, em: Emitter[Int]) => {
      em.emit(1)
      v
    }).combine(_.sum)
    ExecutionPlan.execute(out)
    0
  }

  def wordCountVerbose(args: Array[String]) = {
    val lines = new DistCollection[String](new URI(args(0)))

    val out = lines.flatMap(_.split("\\s").toTraversable).groupBySort((v: String, em: Emitter[Int]) => {
      em.emit(1)
      v
    }).combine(_.sum)

    ExecutionPlan.execute(out)
    println(out)
    0
  }

  def wordCountNative(args: Array[String]) = {
    val lines = new DistCollection[String](new URI(args(0)))

    val out = lines.distDo((el: String, em: Emitter[String]) =>
      el.split("\\s").foreach(v => em.emit(new String("testString".getBytes))))
      .groupBySort((v: String, em: Emitter[Int]) => {
      em.emit(1)
      v
    })

    ExecutionPlan.execute(out)
    0
  }


  def demo(args: Array[String]) = {
    val longCol = new DistCollection[Long](new URI("./kryo-longsTo1m"))
    val longSet = new DistHashSet[Long](new URI("./kryo-longsTo1m"))
    0
  }

  def main(args: Array[String]) = {
    examples(args(0))(args.tail.toArray)
    0
  }
}
