package scala.collection.distributed

import api.dag.{CombinePlanNode, FlattenPlanNode, GroupByPlanNode, ParallelDoPlanNode}
import api.{DistContext, Emitter}
import api.CollectionId
import collection.immutable.GenIterable
import collection.generic.{GenericCompanion}
import mrapi.FSAdapter
import execution.{DCUtil, ExecutionPlan}
import collection.immutable.GenTraversable

/**
 * User: vjovanovic
 * Date: 4/23/11
 */

trait DistIterable[+T]
  extends GenIterable[T]
  with GenericDistTemplate[T, DistIterable]
  with DistIterableLike[T, DistIterable[T], Iterable[T]]
  with CollectionId
{

  def seq = FSAdapter.valuesIterable[T](location)

  override def companion: GenericCompanion[DistIterable] with GenericDistCompanion[DistIterable] = DistIterable

  def stringPrefix = "DistIterable"

  def size = throw new UnsupportedOperationException("Not implemented yet!!!")

  protected[this] def parCombiner = throw new UnsupportedOperationException("Not implemented yet!!!")

  def groupBy[K, B](keyFunction: (T, Emitter[B]) => K): DistMap[K, GenIterable[B]] = {
    // add a groupBy node to execution plan
    val outDistCollection = new DistHashMap[K, GenIterable[B]](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new GroupByPlanNode[T, B, K](outDistCollection, keyFunction))
    ExecutionPlan.sendToOutput(node, outDistCollection)
    ExecutionPlan.execute()
    outDistCollection
  }

   def parallelDo[B](parOperation: (T, Emitter[B], DistContext) => Unit): DistIterable[B] = {
    // add a parallel do node
    val outDistCollection = new DistColl[B](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new ParallelDoPlanNode(outDistCollection, parOperation))
    ExecutionPlan.sendToOutput(node, outDistCollection)
    ExecutionPlan.execute()
    outDistCollection
  }

  def flatten[B >: T](collections: GenTraversable[DistIterable[B]]) = {
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
    val outDistCollection = new DistHashMap[K, C](DCUtil.generateNewCollectionURI)

    val node = ExecutionPlan.addPlanNode(this, new CombinePlanNode[T, K, B, C](outDistCollection, keyFunction, op))
    ExecutionPlan.sendToOutput(node, outDistCollection)

    ExecutionPlan.execute()
    outDistCollection
  }

  def sort[B](by: (T) => Ordered[B]) = throw new UnsupportedOperationException("Not implemented yet!!!")

}

/**$factoryInfo
 */
object DistIterable extends DistFactory[DistIterable] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistIterable[T]] = new GenericCanDistBuildFrom[T]

  def newRemoteBuilder[T] = new IterableRemoteBuilder[T]

  def newBuilder[T] = new IterableRemoteBuilder[T]
}
