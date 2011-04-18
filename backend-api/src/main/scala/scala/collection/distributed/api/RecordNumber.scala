package scala.collection.distributed.api

/**
 * User: vjovanovic
 * Date: 4/5/11
 */

class RecordNumber(val fileNumber: Int = 0, val recordStart: Long = 0L, var counter: Long = 0L) {
  def incrementRecordCounter() = counter += 1
}