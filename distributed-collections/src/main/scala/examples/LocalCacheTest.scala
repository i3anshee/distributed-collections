package examples

import dcollections.DistSet
import java.net.URI
import dcollections.api.Emitter

/**
 * User: vjovanovic
 * Date: 4/18/11
 */

@serializable class A(val a: Int) {
  override def equals(p1: Any) = p1 match {
    case v: A => (v.a == a)
    case _ => false
  }
}

object LocalCacheTest {
  def main(args: Array[String]) = {
    val ds1 = new DistSet[Long](new URI("./longsTo1k"))

    var booleanFirstTime = true
    var localClass: A = new A(0)
    var localByte = 2.byteValue
    var localInt = 2.intValue
    var localDouble = 2.doubleValue
    ds1.parallelDo((v: Long, emitter: Emitter[Long]) => {
      if (booleanFirstTime) {
        booleanFirstTime = false
        if (!(localClass == new A(0))) throw new RuntimeException("Should be A(0)!!!")
        if (localByte != 2) throw new RuntimeException("Should be 2!!!")
        if (localInt != 2) throw new RuntimeException("Should be 2!!!")
        if (localDouble != 2.0) throw new RuntimeException("Should be 2.0!!!")
      }
      localClass = new A(1)
      localByte = 3
      localInt = 4
      localDouble = 5.toDouble

      if (!(localClass == new A(1))) throw new RuntimeException("Should be A(2)!!!")
      if (localByte != 3) throw new RuntimeException("Should be 3!!!")
      if (localInt != 4) throw new RuntimeException("Should be 4!!!")
      if (localDouble != 5.0) throw new RuntimeException("Should be 5.0!!!")
    })
  }
}