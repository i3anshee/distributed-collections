package examples

import java.net.URI
import collection.distributed._
import io.KryoSerializer

/**
 * User: vjovanovic
 * Date: 4/26/11
 */


object DistCollTest {

  val examples: Map[String, (Array[String]) => Int] = Map(
    ("Demo" -> demo),("Wc" -> wc)
  )

  def demo(args: Array[String]) = {
    val longCol = new DistCollection[Long](new URI("./kryo-longsTo1m"))
    val longSet = new DistHashSet[Long](new URI("./kryo-longsTo1m"))

    val materialized1 = longCol.map(v => 0 to 2).filter(v => true)

    println(materialized1.mkString)
    0
  }

  def wc(args: Array[String]) = {
    val text = DistCollection[String]("collections/text")
    val wordCount = text.flatMap(_.split("\\s"))
    0
  }

  def main(args: Array[String])  {
    examples(args(0))(args.tail.toArray)
    0
  }
}
