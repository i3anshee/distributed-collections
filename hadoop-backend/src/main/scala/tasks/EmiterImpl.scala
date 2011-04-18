package tasks

import scala.collection.distributed.api.Emitter
import collection.mutable.{ArrayBuffer}

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

