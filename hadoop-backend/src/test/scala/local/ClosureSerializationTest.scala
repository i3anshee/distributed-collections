package local

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test

/**
 * User: vjovanovic
 * Date: 3/12/11
 */

class ClosureSerializationTest extends TestNGSuite with ShouldMatchers {

  @Test
  def serializeDeserializeTest() {

    val intVar: Int = 5
    var list = List(1, 2, 3)

    // function to serialize
    val initialFunction = (x: Int) => list.reduceLeft[Int](_ + _ + x + intVar)
    val initialResult = initialFunction(5)

    // serialize to byte array input stream
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos writeObject (initialFunction)
    oos flush

    // deserialize function
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val deserializedFunction = ois.readObject


    val deserializedResult = deserializedFunction.asInstanceOf[(Int) => Int] (5)

    initialResult should equal(deserializedResult)

  }

}