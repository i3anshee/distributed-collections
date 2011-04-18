package scala.collection.distributed.api

import scala.collection.mutable
import scala.collection.immutable

/**
 * User: vjovanovic
 * Date: 4/7/11
 */

class DistContext(val globalCache: immutable.Map[String, Any]) {
  var recordNumber: RecordNumber = new RecordNumber()
}