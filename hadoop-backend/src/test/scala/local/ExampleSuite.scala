package local

import collection.mutable.ListBuffer
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.{BeforeMethod, Test}

/**
 * User: vjovanovic
 * Date: 3/11/11
 */

class ExampleSuite extends TestNGSuite with ShouldMatchers {

  var sb: StringBuilder = _
  var lb: ListBuffer[String] = _

  @BeforeMethod
  def initialize() {
    sb = new StringBuilder("ScalaTest is ")
    lb = new ListBuffer[String]
  }

  @Test def verifyEasy() { // Uses ScalaTest assertions
    sb.append("easy!")
    assert(sb.toString === "ScalaTest is easy!")
    assert(lb.isEmpty)
    lb += "sweet"
    intercept[StringIndexOutOfBoundsException] {
      "concise".charAt(-1)
    }
  }

  @Test def verifyFun() { // Uses ScalaTest matchers
    sb.append("fun!")
    sb.toString should be ("ScalaTest is fun!")
    lb should be ('empty)
    lb += "sweet"
    evaluating { "concise".charAt(-1) } should produce [StringIndexOutOfBoundsException]
  }
}