package scala.collection.distributed.shared

import java.util.concurrent.atomic.AtomicLong
import collection.{immutable, mutable}
import immutable.GenSeq

/**
 * @author Vojin Jovanovic
 */

abstract class DistSideEffects(val varType: DSEType) extends Serializable {
  val id = DistSideEffects.newId
  DistSideEffects.add(this)
}

object DistSideEffects {
  val idSeed = new AtomicLong(0)
  val shared = mutable.HashSet[DistSideEffects]()

  def newId = idSeed.getAndIncrement

  def add(v: DistSideEffects) = shared += v

}

// provided with optional combiner
class DSECollection[T](combiner: Option[(Iterator[T] => T)], process: Option[Iterator[T] => GenSeq[T]])
  extends DistSideEffects(CollectionType) {
  def put(value: T) = {}

  // write to buffer
  def seq = {}

  // return seq
  def toMap[K, V](implicit ev: T <:< (K, V)): immutable.Map[K, V] = Map[K, V]()
}

// must have both combiner and aggregator
class DSEVar[T](combiner: (Iterator[T] => T), aggregator: (Iterator[T] => T)) extends DistSideEffects(VarType) {
  def set(value: T) = {}

  //  def apply():T = {}
}

// will be mapped to mr counters and spark accumulator vars
class DSECounter extends DistSideEffects(CounterType) {
  def increment = {}
}


sealed class DSEType

case object CollectionType extends DSEType

case object VarType extends DSEType

case object CounterType extends DSEType
