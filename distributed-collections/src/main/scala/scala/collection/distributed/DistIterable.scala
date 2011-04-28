package scala.collection.distributed

import api.dag._
import api.{DistContext, Emitter}
import api.CollectionId
import collection.immutable.{GenIterable, GenTraversable}
import collection.generic.{GenericCompanion}
import mrapi.FSAdapter
import execution.{DCUtil, ExecutionPlan}
import io.CollectionsIO

trait DistIterable[+T]
  extends GenIterable[T]
  with GenericDistTemplate[T, DistIterable]
  with DistIterableLike[T, DistIterable[T], Iterable[T]]
  with CollectionId {
  def seq = FSAdapter.valuesIterable[T](location)

  override def companion: GenericCompanion[DistIterable] with GenericDistCompanion[DistIterable] = DistIterable

  def stringPrefix = "DistIterable"

  lazy val sizeLongVal: Long = CollectionsIO.getCollectionMetaData(this).size

  def size = if (sizeLong > Int.MaxValue) throw new RuntimeException("Size is larger than MAX_INT!!!") else sizeLong.toInt

  def sizeLong = sizeLongVal

  def nonEmpty = size != 0

  override def isEmpty = sizeLong != 0

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


  def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T] = {
    val outDistColl = new DistColl[T](DCUtil.generateNewCollectionURI)
    val node = ExecutionPlan.addFlattenNode(
      new FlattenPlanNode(outDistColl, List(this) ++ collections)
    )
    ExecutionPlan.sendToOutput(node, outDistColl)
    ExecutionPlan.execute()
    outDistColl
  }

  def sgbr[S, K, T1, T2, That](by: (T) => Ordered[S] = nullOrdered,
                               keyFunction: (T, Emitter[T1]) => K = nullKey,
                               reduceOp: (T2, T1) => T2 = nullReduce)
                              (implicit sgbrResult: ToSGBRColl[T, K, T1, T2, That]): That = {

    if (by == nullOrdered && keyFunction == nullKey)
      throw new RuntimeException("At least one parameter must be specified!!")

    if (keyFunction == nullKey && reduceOp != nullReduce)
      throw new RuntimeException("In order to reduce, key function must be specified.")

    val result = sgbrResult.result(DCUtil.generateNewCollectionURI)

    var input: CollectionId = this
    var output = CollectionId(DCUtil.generateNewCollectionURI)
    if (by != nullOrdered) {
      ExecutionPlan.addPlanNode(input, new SortPlanNode[T, S](output, by))
      input = output
      output = CollectionId(DCUtil.generateNewCollectionURI)
    }

    if (keyFunction != nullKey) {
      ExecutionPlan.addPlanNode(output, new GroupByPlanNode(input, keyFunction))
      input = output
      output = CollectionId(DCUtil.generateNewCollectionURI)
    }

    if (reduceOp != nullReduce) {
      ExecutionPlan.addPlanNode(output, new CombinePlanNode(input, reduceOp))
      input = output
      output = CollectionId(DCUtil.generateNewCollectionURI)
    }

    sgbrResult.result(output.location)
  }


  protected[this] def parCombiner = throw new UnsupportedOperationException("Not implemented yet!!!")
}

/**$factoryInfo
 */
object DistIterable extends DistFactory[DistIterable] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistIterable[T]] = new GenericCanDistBuildFrom[T]

  def newRemoteBuilder[T] = new IterableRemoteBuilder[T]

  def newBuilder[T] = new IterableRemoteBuilder[T]
}
