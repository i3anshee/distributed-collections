package local

import org.scalatest.testng.TestNGSuite
import org.scalatest.matchers.ShouldMatchers
import org.testng.annotations.Test
import io.KryoSerializer

/**
 * @author Vojin Jovanovic
 */

/**
 * User: vjovanovic
 * Date: 3/12/11
 */

class KryoTest extends TestNGSuite with ShouldMatchers {

  @Test
  def serializeDeserializeTest() {

    // list to serialize
    val initialValue = List(1, 2, 3, 4, 5, "Test String", (1L, "String in tuple"))

    val si = new KryoSerializer().newInstance()

    val afterSer = si.deserialize(si.serialize(initialValue))

    initialValue should equal(afterSer)
  }

}