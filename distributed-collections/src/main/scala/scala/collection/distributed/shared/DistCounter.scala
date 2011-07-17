package scala.collection.distributed.shared

import collection.distributed.api.shared._

/**
 * @author Vojin Jovanovic
 */

class DistCounter(private var value: Long = 0L) extends DistSideEffects(CounterType) with DistCounterLike {
  def apply() = value

  def +=(n1: Long) = value += n1
}

object DistCounter {
  def apply(initial: Long): DistCounterLike = {
    val proxy = new DistCounterProxy(new DistCounter(initial))
    DistSideEffects.add(proxy)
    proxy
  }

  def apply(): DistCounterLike = apply(0L)
}