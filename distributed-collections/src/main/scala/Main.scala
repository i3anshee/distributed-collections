import dcollections.api.Emitter
import execution.ExecutionPlan
import java.net.URI
import scala.util.Random

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object Main {
  def main(args: Array[String]) = {
    val someValue = Random.nextInt.abs % 100

    val distributedSet = new dcollections.DistSet[Long](new URI("long-set"))

    ExecutionPlan.globalCache.put("testGlobalCache", 1)
    val reduced = distributedSet.combineValues((v, emitter: Emitter[Long]) => {
      emitter.emit(v)
      1L
    },
      (agg: Long, v: Long) => agg + v
    )
    println("Reduced = " + reduced.toString)

    val distributedSet1 = new dcollections.DistSet[Long](new URI("long-set1"))
    println("Random Value = " + someValue)

    // value containing closures
    val generatedSet = distributedSet.map(_ + someValue)
    println(generatedSet.size)

    val flatten = generatedSet.flatten(List(distributedSet1))

    println(flatten.toString)
  }
}