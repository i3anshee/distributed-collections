package scala.collection.distributed.shared

import collection.distributed.api.shared.{RecordType, DistSideEffects, DistRecordCounterProxy, RecordCounterLike}

/**
 * @author Vojin Jovanovic
 */

private[this] class DistRecordCounter extends DistSideEffects(RecordType) with RecordCounterLike {
  def counter = throw new UnsupportedOperationException("Client side record counting not possible!!!")

  def recordStart = throw new UnsupportedOperationException("Client side record counting not possible!!!")

  def fileNumber = throw new UnsupportedOperationException("Client side record counting not possible!!!")
}

object DistRecordCounter {
  def apply(): RecordCounterLike = {
    val proxy = new DistRecordCounterProxy(new DistRecordCounter())
    DistSideEffects.add(proxy)
    proxy
  }
}