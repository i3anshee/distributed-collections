package scala.colleciton.distributed.hadoop.shared

import collection.distributed.api.shared.{DSECounterLike, CounterType, DistSideEffects}
import org.apache.hadoop.mapreduce.Counter

/**
 * @author Vojin Jovanovic
 */

class DSECounterNode(val initialValue: Long = 0, val counter: Counter) extends DistSideEffects(CounterType) with DSECounterLike {

  def apply() = initialValue + counter.getValue

  def +=(n: Long) = counter.increment(n)
}