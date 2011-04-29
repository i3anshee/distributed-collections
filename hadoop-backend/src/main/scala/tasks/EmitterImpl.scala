package tasks

import collection.mutable.{ArrayBuffer}
import collection.distributed.api.{CollectionId, UntypedEmitter}
import collection.immutable.GenTraversable

class EmitterImpl(val collections: GenTraversable[CollectionId]) extends UntypedEmitter {
  val collectionBuffers = collections.map(coll => (coll, new ArrayBuffer[Any])).toSeq

  def emit(index: Int, el: Any) = collectionBuffers(index)._2 += el

  def clear(index: Int) = collectionBuffers(index)._2.clear

  def clear() = collectionBuffers.foreach(_._2.clear)

  def getBuffer(index: Int) = collectionBuffers(index)._2

}

