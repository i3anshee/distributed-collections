import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

object Main {
  def main(args: Array[String]) = {

    val distributedSet = new dcollections.Set[Long](new URI("collection"));

    val generatedSet = distributedSet.map(_ + 1)
  }
}