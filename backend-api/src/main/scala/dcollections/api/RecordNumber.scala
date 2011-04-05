package dcollections.api

/**
 * User: vjovanovic
 * Date: 4/5/11
 */

class RecordNumber(val fileNumber: Int, val recordStart: Long, var counter: Long) {
  def nextRecord() = counter += 1
}