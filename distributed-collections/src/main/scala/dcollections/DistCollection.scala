package dcollections

import api.dag.{FlattenPlanNode, ParallelDoPlanNode, GroupByPlanNode}
import api.{CollectionId, RecordNumber, Emitter}
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
class DistCollection[A](location: URI) extends CollectionId(location) {

  def parallelDo[B](parOperation: (A, Emitter[B], RecordNumber) => Unit): DistCollection[B] = {
    // add a parallel do node
    val outDistCollection = new DistCollection[B](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new ParallelDoPlanNode(outDistCollection, parOperation))
    ExecutionPlan.sendToOutput(node, outDistCollection)

    outDistCollection
  }

  def parallelDo[B](parOperation: (A, Emitter[B]) => Unit): DistCollection[B] = {
    parallelDo((a: A, emitter: Emitter[B], rec: RecordNumber) => parOperation(a, emitter))
  }

  def groupBy[K, B](keyFunction: (A, Emitter[B]) => K): DistCollection[Pair[K, Iterable[B]]] = {
    // add a groupBy node to execution plan
    val outDistCollection = new DistCollection[Pair[K, Iterable[B]]](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new GroupByPlanNode[A, B, K](outDistCollection, keyFunction))
    ExecutionPlan.sendToOutput(node, outDistCollection)

    outDistCollection
  }

  def groupBy[K](keyFunction: A => K): DistCollection[Pair[K, Iterable[A]]] =
    groupBy((el, emitter) => {
      emitter.emit(el)
      keyFunction(el)
    })

  def flatten(collections: Traversable[DistCollection[A]]): DistCollection[A] = {
    val outDistCollection = new DistCollection[A](DCUtil.generateNewCollectionURI)
    val node = ExecutionPlan.addFlattenNode(
      new FlattenPlanNode(outDistCollection, List(this) ++ collections)
    )
    ExecutionPlan.sendToOutput(node, outDistCollection)
    outDistCollection
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