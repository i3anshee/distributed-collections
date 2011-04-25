package scala.collection.distributed

import api.RecordNumber
import scala.collection.immutable

/**
 * User: vjovanovic
 * Date: 4/19/11
 */

/**
 * Super-trait for all collections that use parallelDo.
 *  Methods from this trait will automatically be replaced on the backend side with meaningful values.
 */
trait DistRecords {
  /**
   * Yields the element of a collection.
   */
  def emit[A](elem: A) = null

  /**
   * Returns the number of currently processed record.
   */
  def record: RecordNumber = null

  /**
   * Gives access to the global cache.
   */
  def cache: immutable.Map[String, Any] = null
}