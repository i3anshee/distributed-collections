package scala.collection.distributed.api

/**
 * User: vjovanovic
 * Date: 4/5/11
 */

class RecordNumber(val filePart: Long = 0L, var counter: Long = 0L) extends Ordered[RecordNumber] {

  def incrementRecordCounter() = counter += 1

  def compare(that: RecordNumber) = RecordNumber.ordering.compare(this, that)

  def fileNumber = (filePart >> RecordNumber.recordStartBits).asInstanceOf[Int]

  def recordStart = filePart & RecordNumber.recordStartMask
}

object RecordNumber {

  private val recordStartBits = 40
  private val recordStartMask = (0xFFFFFFFFFFFFFFFFL >>> (64 - recordStartBits))
  private val fileNumberSize = 0xFFFFFFFFFFFFFFFFL  >>> recordStartBits

  def apply(fileNumber: Int = 0, recordStart: Long = 0L, counter: Long = 0L) = {
    if (fileNumber > fileNumberSize) throw new IllegalArgumentException("fileNumber must be less than 0xFFFFFF\n" + fileNumber + "\n" + fileNumberSize)
    if (recordStart > recordStartMask) throw new IllegalArgumentException("recordStart must be less than 0xFFFFFFFFFFFFFF" + recordStart)

    new RecordNumber((fileNumber.toLong << recordStartBits) | recordStart, counter)
  }

  implicit def or: Ordering[RecordNumber] = ordering

  val ordering = new Ordering[RecordNumber] {
    def compare(x: RecordNumber, y: RecordNumber) = {
      val o1 = unsignedCompare(x.filePart, y.filePart)
      if (o1 == 0) {
        x.counter.compare(y.counter)
      } else
        o1
    }
  }

  private[this] def unsignedCompare(n1: Long, n2: Long) = if (n1 == n2)
    0
  else if ((n1 < n2) ^ ((n1 < 0) != (n2 < 0)))
    -1
  else 1
}