package scala.colleciton.distributed.hadoop.shared

import collection.distributed.api.shared._

/**
 * @author Vojin Jovanovic
 */

object DSENodeFactory {
  def createNode(data: (DistSideEffects with DSEProxy[_], Array[Byte])): DistSideEffects with DSEProxy[_] = {
    data._1.varType match {
      case CollectionType => throw new UnsupportedOperationException("tomorrow")
      case VarType =>  throw new UnsupportedOperationException("tomorrow")
      case CounterType => throw new UnsupportedOperationException("tomorrow")
    }
  }
}