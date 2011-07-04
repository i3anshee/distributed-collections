package scala.collection.distributed.shared

import collection.distributed.api.shared._

/**
 * @author Vojin Jovanovic
 */

class DSECounter(private var value: Long = 0L) extends DistSideEffects(CounterType) with DSECounterLike {
  def apply() = value

  def +=(n1: Long) = value += n1
}

object DSECounter {
  def apply(initial: Long): DSECounterLike = {
    val proxy = new DSECounterProxy(new DSECounter(initial))
    DistSideEffects.add(proxy)
    proxy
  }

  def apply(): DSECounterLike = apply(0L)
}