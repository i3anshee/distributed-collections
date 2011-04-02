import java.net.URI
import scala.util.Random

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object Main {
  def main(args: Array[String]) = {
    val someValue = Random.nextInt.abs % 100

    // TODO from database, memory, abstraction ?
    val distributedSet = new dcollections.DistSet[Long](new URI("collection-serialized"));
    println("Random Value = " + someValue)

    // value containing closures
    val generatedSet = distributedSet.filter(_ < someValue)

    println("Reduced value (SUM) =" + generatedSet.reduce((a: Long, b: Long) => a + b))

    // test of printing
    println("Original set:")
    println(distributedSet.toString)

    println("New original.map(_ + 1):")
    println(generatedSet.toString)


    // checking multiple operations
    println("Multiple operations original.map(_ * 123.123).filter(_ > 400):")
    println(generatedSet.map(_ * 123.123).filter(_ > 400).toString)



    // test set property
    val noDuplicatesSet = distributedSet.map(_ % 2)
    println(noDuplicatesSet.toString)

  }
}