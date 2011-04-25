package scala.collection.distributed

import api.dag.{CombinePlanNode, FlattenPlanNode, GroupByPlanNode, ParallelDoPlanNode}
import api.{DistContext, Emitter, CollectionId}
import java.net.URI
import execution.{DCUtil, ExecutionPlan}
import collection.mutable.GenTraversable
import collection.immutable.GenIterable

class DistColl[T](uri: URI)
  extends DistIterable[T]
  with GenericDistTemplate[T, DistColl]
  with CollectionId {

  override def location = uri

  def parallelDo[B](parOperation: (T, Emitter[B], DistContext) => Unit): DistIterable[B] = {
    // add a parallel do node
    val outDistCollection = new DistColl[B](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new ParallelDoPlanNode(outDistCollection, parOperation))
    ExecutionPlan.sendToOutput(node, outDistCollection)
    ExecutionPlan.execute()
    outDistCollection
  }

  def groupBy[K, B](keyFunction: (T, Emitter[B]) => K): DistMap[K, GenIterable[B]] = {
    // add a groupBy node to execution plan
    val outDistCollection = new DistMap[K, GenIterable[B]](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new GroupByPlanNode[T, B, K](outDistCollection, keyFunction))
    ExecutionPlan.sendToOutput(node, outDistCollection)
    ExecutionPlan.execute()
    outDistCollection
  }

  def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T] = {
    val outDistCollection = new DistColl[T](DCUtil.generateNewCollectionURI)
    val node = ExecutionPlan.addFlattenNode(
      new FlattenPlanNode(outDistCollection, List(this) ++ collections)
    )
    ExecutionPlan.sendToOutput(node, outDistCollection)
    ExecutionPlan.execute()
    outDistCollection
  }

  def combineValues[K, B, C](keyFunction: (T, Emitter[B]) => K, op: (C, B) => C): DistMap[K, C] = {
    // add combine node
    val outDistCollection = new DistMap[K, C](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new CombinePlanNode[T, K, B, C](outDistCollection, keyFunction, op))
    ExecutionPlan.sendToOutput(node, outDistCollection)

    ExecutionPlan.execute()
    outDistCollection
  }
}

object DistColl extends DistFactory[DistColl] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistColl[T]] = new GenericCanDistBuildFrom[T]

  def newRemoteBuilder[T] = new DistCollRemoteBuilder[T]

  def newBuilder[T] = new DistCollRemoteBuilder[T]
}