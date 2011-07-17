package scala.colleciton.distributed.hadoop.shared

import collection.distributed.api.shared.{DistCounterLike, CounterType, DistSideEffects}
import org.apache.hadoop.mapreduce.Counter

/**
 * @author Vojin Jovanovic
 */

class DistCounterNode(val initialValue: Long = 0, val counter: Counter) extends DistSideEffects(CounterType) with DistCounterLike {

  def apply() = initialValue + counter.getValue

  def +=(n: Long) = counter.increment(n)
}