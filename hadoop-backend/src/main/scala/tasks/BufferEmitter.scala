package tasks

import collection.mutable.{ArrayBuffer}
import collection.distributed.api.{UntypedEmitter}

class BufferEmitter(val nr: Int) extends UntypedEmitter {
  val buffers = Seq.fill(nr)(new ArrayBuffer[Any])

  def emit(index: Int, el: Any) = buffers(index) += el

  def clear(index: Int) = buffers(index).clear

  def clear() = buffers.foreach(_.clear)

  def getBuffer(index: Int) = buffers(index)

}

