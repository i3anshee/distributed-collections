package dcollections

import api.dag.{ParallelDoPlanNode, GroupByPlanNode}
import api.Emitter
import java.util.UUID
import java.net.URI
import execution.{DCUtil, ExecutionPlan}

/**
 * User: vjovanovic
 * Date: 3/21/11
 */

/**
 * Super class for all distributed collections. Provides four basic primitives from which all other methods are built.
 */
class DistCollection[A](var location: URI) {
  val uID: UUID = UUID.randomUUID

  def parallelDo[B](parOperation: (A, Emitter[B]) => Unit): DistCollection[B] = {
    // add a parallel do node
    val outDistCollection = new DistCollection[B](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new ParallelDoPlanNode(parOperation))
    ExecutionPlan.sendToOutput(node, outDistCollection.location)

    outDistCollection
  }

  def groupBy[K, B](keyFunction: (A, Emitter[B]) => K): DistCollection[Pair[K, Iterable[B]]] = {
    // add a groupBy node to execution plan
    val outDistCollection = new DistCollection[Pair[K, Iterable[B]]](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new GroupByPlanNode[A, B, K](keyFunction))
    ExecutionPlan.sendToOutput(node, outDistCollection.location)

    outDistCollection
  }

  def groupBy[K](keyFunction: A => K): DistCollection[Pair[K, Iterable[A]]] =
    groupBy((el, emitter) => {
      emitter.emit(el)
      keyFunction(el)
    })

  def flatten(collections: Traversable[DistCollection[A]]): DistCollection[A] = {
    // add flatten node
    this
  }

  //  def combineValues[K, B](keyFunction: A => K, op: (B, A) => B): DistCollection[Pair[K, B]] = {
  //    add combine node
  //    val outDistCollection = new DistCollection[Pair[K, B]](DCUtil.generateNewCollectionURI)
  //
  //    val node = ExecutionPlan.addPlanNode(this, new CombinePlanNode[A, B, K](keyFunction, op))
  //    ExcutionPlan.sendToOutput(node, outDistCollection.location)
  //
  //    outDistCollection
  //  }


  //  def view(): DistCollection[A] = new DistCollection[A]
}