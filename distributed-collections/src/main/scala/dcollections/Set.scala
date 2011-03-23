package dcollections

import execution.{DCUtil, ExecutionPlan}
import java.net.URI
import mrapi.FSAdapter
import execution.dag._

/**
 * User: vjovanovic
 * Date: 3/13/11
 */

class Set[A](val collectionFile: URI) extends DistributedCollection() {

  def location = collectionFile

  def map[B](f: A => B): Set[B] = {
    val inputNode = ExecutionPlan.addInputCollection(this)

    val mapNode = ExecutionPlan.addOperation(inputNode, new MapClosurePlanNode[A, B](f))
    val reduceNode = ExecutionPlan.addOperation(mapNode, new ReduceSetPlanNode())

    val outputSet = new Set[B](DCUtil.generateNewCollectionURI)
    ExecutionPlan.sendToOutput(reduceNode, outputSet.location)

    ExecutionPlan.execute
    outputSet
  }

  def filter(p: A => Boolean): Set[A] = {
    val inputNode = ExecutionPlan.addInputCollection(this)

    val mapNode = ExecutionPlan.addOperation(inputNode, new MapFilterClosurePlanNode[A](p))
    val reduceNode = ExecutionPlan.addOperation(mapNode, new ReduceSetPlanNode())

    val outputSet = new Set[A](DCUtil.generateNewCollectionURI)
    ExecutionPlan.sendToOutput(reduceNode, outputSet.location)

    ExecutionPlan.execute
    outputSet
  }

  def reduce[B, A](op: (A, B) => B): B = {
    val inputNode = ExecutionPlan.addInputCollection(this)

    val mapNode = ExecutionPlan.addOperation(inputNode, new MapReducePlanNode())
    val reduceNode = ExecutionPlan.addOperation(mapNode, new ReduceClosurePlanNode(op))

    val outputURI = DCUtil.generateNewCollectionURI
    ExecutionPlan.sendToOutput(reduceNode, outputURI)

    ExecutionPlan.execute

    // read iterable from file system and apply reduce locally
    val result = FSAdapter.valuesIterable[B](outputURI).last

    // cleanup the file containing the final result
    FSAdapter.remove(outputURI)

    result
  }

  override def toString(): String = {
    val builder = new StringBuilder("[ ")
    FSAdapter.valuesIterable[A](location).foreach((v: A) => builder.append(v).append(" "))
    builder.append("]")
    builder.toString
  }
}
