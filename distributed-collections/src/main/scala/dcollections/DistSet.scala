package dcollections

import api.Emitter
import execution.{ExecutionPlan}
import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/13/11
 */

class DistSet[A](val collectionFile: URI) extends DistCollection[A] {

  def location = collectionFile

  def map[B](f: A => B): DistSet[B] = {
    val resultCollection = parallelDo((elem: A, emitter: Emitter[B]) => {
      emitter.emit(f(elem))
    }).groupBy(_.hashCode)
      .parallelDo((pair: (Int, scala.Traversable[B]), emitter: Emitter[B]) => {
      val existing = Set()
      trav.foreach((if (!existing.contains(_)) {
        existing ++ _
        emitter.emit(pair._2)
      }))
    })

    ExecutionPlan.execute()

    new DistSet[B](resultCollection.location)
  }

  //  def filter(p: A => Boolean): DistSet[A] = {
  //    val inputNode = ExecutionPlan.addInputCollection(this)
  //
  //    val mapNode = ExecutionPlan.addOperation(inputNode, new MapFilterClosurePlanNode[A](p))
  //    val reduceNode = ExecutionPlan.addOperation(mapNode, new ReduceSetPlanNode())
  //
  //    val outputSet = new DistSet[A](DCUtil.generateNewCollectionURI)
  //    ExecutionPlan.sendToOutput(reduceNode, outputSet.location)
  //
  //    ExecutionPlan.execute
  //    outputSet
  //  }
  //
  //  def reduce[B, A](op: (A, B) => B): B = {
  //    val inputNode = ExecutionPlan.addInputCollection(this)
  //
  //    val mapNode = ExecutionPlan.addOperation(inputNode, new MapReducePlanNode())
  //    val reduceNode = ExecutionPlan.addOperation(mapNode, new ReduceClosurePlanNode(op))
  //
  //    val outputURI = DCUtil.generateNewCollectionURI
  //    ExecutionPlan.sendToOutput(reduceNode, outputURI)
  //
  //    ExecutionPlan.execute
  //
  //    // read iterable from file system and apply reduce locally
  //    val result = FSAdapter.valuesIterable[B](outputURI).last
  //
  //    // cleanup the file containing the final result
  //    FSAdapter.remove(outputURI)
  //
  //    result
  //  }
  //
  //  override def toString(): String = {
  //    val builder = new StringBuilder("[ ")
  //    FSAdapter.valuesIterable[A](location).foreach((v: A) => builder.append(v).append(" "))
  //    builder.append("]")
  //    builder.toString
  //  }
}
