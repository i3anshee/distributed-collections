package dcollections

import execution.{DCUtil, ExecutionPlan}
import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/13/11
 */

class Set[A](val collectionFile: URI) extends DistributedCollection() {

  def location = collectionFile

  def map[B](f: A => B): Set[B] = {
    val inputNode = ExecutionPlan.addInputCollection(this)

    val mapNode = ExecutionPlan.addMapOperation(inputNode, f)

    val outputSet = new Set[B](DCUtil.generateNewCollectionURI)

    ExecutionPlan.sendToOutput(mapNode, DCUtil.generateNewCollectionURI)

    ExecutionPlan.execute
    outputSet
  }
}