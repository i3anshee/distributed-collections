package scala.colleciton.distributed.hadoop.shared

import collection.distributed.api.shared._
import java.nio.ByteBuffer
import org.apache.hadoop.mapred.{Reporter, JobConf}

/**
 * @author Vojin Jovanovic
 */

object DSENodeFactory {
  def initializeNode(reporter: Reporter, data: (DistSideEffects with DSEProxy[_], Array[Byte])): Unit = {
    data._1.varType match {
      case CollectionType => throw new UnsupportedOperationException("Not imlemented yet!!!")
      case VarType => throw new UnsupportedOperationException("Not implemented yet!!!")
      case CounterType => {
        val buffer = ByteBuffer.allocate(8)
        buffer.put(data._2)
        data._1.asInstanceOf[DSECounterProxy].impl =
          new DSECounterNode(buffer.getLong(0), reporter.getCounter("DSECounter", data._1.uid.toString))
      }
    }
  }
}