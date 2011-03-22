import java.net.URI
import scala.util.Random

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object Main {
  def main(args: Array[String]) = {
    val someValue = Random.nextInt.abs % 100
    val distributedSet = new dcollections.Set[Long](new URI("collection-serialized"));
    println("Random Value = " + someValue)

    val generatedSet = distributedSet.filter(_ < someValue)

    println("Original set:")
    println(distributedSet.toString)

    println("New original.map(_ + 1):")
    println(generatedSet.toString)

    println("Multiple operations original.map(_ * 123.123).filter(_ > 400):")
    println(generatedSet.map(_ * 123.123).filter(_ > 400).toString)

  }
}