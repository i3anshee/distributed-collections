import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object Main {
  def main(args: Array[String]) = {

    val distributedSet = new dcollections.Set[Long](new URI("collection-serialized"));

    val generatedSet = distributedSet.map(_ + 1)

    println("Original set:")
    println(distributedSet.toString)

    println("New original.map(_+1):")
    println(generatedSet.toString)

  }
}