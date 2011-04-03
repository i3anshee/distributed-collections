package tasks

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.{BytesWritable}
import dcollections.api.Emitter
import scala.{None}
import collection.mutable.{Buffer, ArrayBuffer}

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

class EmiterImpl extends Emitter[AnyRef] {
  val buffer = ArrayBuffer[AnyRef]()

  def emit(elem: AnyRef) = buffer += elem

  def clear = buffer.clear

  def getBuffer = buffer.toBuffer
}

