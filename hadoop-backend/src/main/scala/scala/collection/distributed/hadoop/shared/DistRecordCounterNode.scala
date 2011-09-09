package scala.collection.distributed.hadoop.shared

import collection.distributed.api.RecordNumber
import collection.distributed.api.shared.{DistSideEffects, RecordType, RecordCounterLike}

/**
 * @author Vojin Jovanovic
 */
//TODO (VJ) superfluous indirection (FIX)
class DistRecordCounterNode(override val recordNumber: RecordNumber) extends DistSideEffects(RecordType) with RecordCounterLike {
  def counter = recordNumber.counter

  def recordStart = recordNumber.recordStart

  def fileNumber = recordNumber.fileNumber
}