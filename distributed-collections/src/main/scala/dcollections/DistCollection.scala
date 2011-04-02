package dcollections

import api.dag.GroupByPlanNode
import api.Emitter
import java.util.UUID
import java.net.URI
import execution.{DCUtil, ExecutionPlan}
import execution.dag.{CombinePlanNode, ParallelDoPlanNode}

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

/**
 * Super class for all distributed collections. Provides four basic primitives from which all other methods are built.
 */
trait DistCollection[A] {
  val uID: UUID = UUID.randomUUID

  def location: URI

  def parallelDo[B](parOperation: (A, Emitter[B]) => Unit): DistCollection[B] = {
    // add a parallel do node
    val outDistCollection = new DistCollection[B](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new ParallelDoPlanNode(parOperation))
    ExcutionPlan.sendToOutput(node, outDistCollection.location)


    outDistCollection
  }

  def combineValues[K, B](keyFunction: A => K, op: (B, A) => B): DistCollection[Pair[K, B]] = {
    // add combine node
    val outDistCollection = new DistCollection[Pair[K, B]](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new CombinePlanNode[A, B, K](keyFunction, op))
    ExcutionPlan.sendToOutput(node, outDistCollection.location)

    outDistCollection
  }

  def groupBy[K](keyFunction: A => K): DistCollection[K, Traversable[A]] = {
    // add a groupBy node to execution plan
    val outDistCollection = new DistCollection[Pair[K, Traversable[A]]](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new GroupByPlanNode[A, K](keyFunction))
    ExcutionPlan.sendToOutput(node, outDistCollection.location)

    outDistCollection
  }

  def flatten(collections: Traversable[DistCollection[A]]): DistCollection[A] = {
    // add flatten node
  }
}