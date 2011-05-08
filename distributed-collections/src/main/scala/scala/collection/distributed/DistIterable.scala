package scala.collection.distributed

import api._
import api.dag._
import collection.generic.{GenericCompanion}
import scala.colleciton.distributed.hadoop.FSAdapter
import execution.{DCUtil, ExecutionPlan}
import _root_.io.CollectionsIO
import collection.immutable.{GenSeq, GenIterable, GenTraversable}

trait DistIterable[+T]
  extends GenIterable[T]
  with GenericDistTemplate[T, DistIterable]
  with DistIterableLike[T, DistIterable[T], Iterable[T]]
  with ReifiedDistCollection {
  def seq = FSAdapter.valuesIterable[T](location)

  override def companion: GenericCompanion[DistIterable] with GenericDistCompanion[DistIterable] = DistIterable

  def stringPrefix = "DistIterable"

  lazy val sizeLongVal: Long = CollectionsIO.getCollectionMetaData(this).size

  def size = if (sizeLong > Int.MaxValue) throw new RuntimeException("Size is larger than MAX_INT!!!") else sizeLong.toInt

  def sizeLong = sizeLongVal

  def nonEmpty = size != 0

  override def isEmpty = sizeLong != 0


  def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T] = {
    val outDistColl = new DistColl[T](DCUtil.generateNewCollectionURI)
    val allCollections = List(this) ++ collections
    ExecutionPlan.addPlanNode(allCollections, new FlattenPlanNode(allCollections, elemType), List(outDistColl))
    //    ExecutionPlan.execute(outDistColl)
    outDistColl
  }

  def sgbr[S, K, T1, T2, That](by: (T) => Ordered[S] = NullOrdered,
                               key: (T, Emitter[T1]) => K = NullKey,
                               reduce: (T2, T1) => T2 = nullReduce)
                              (implicit sgbrResult: ToSGBRColl[T, K, T1, T2, That]): That = {
    // TODO (VJ) implicit type information (consult with Alex)
    val km = manifest[Any]
    val sm = manifest[Any]
    val t1m = manifest[Any]
    val t2m = manifest[Any]
    val kvp = manifest[(Any, Any)]

    if (by == NullOrdered && key == NullKey)
      throw new RuntimeException("At least one parameter must be specified!!")

    if (key == NullKey && reduce != nullReduce)
      throw new RuntimeException("In order to reduce, key function must be specified.")

    val result = sgbrResult.result(DCUtil.generateNewCollectionURI)

    var input: ReifiedDistCollection = this

    if (by != NullOrdered) {
      var output = ReifiedDistCollection(DCUtil.generateNewCollectionURI, elemType)
      ExecutionPlan.addPlanNode(input, new SortPlanNode[T, S](by, sm), output)
      input = output
    }

    if (key != NullKey) {
      var output = ReifiedDistCollection(DCUtil.generateNewCollectionURI, kvp)
      ExecutionPlan.addPlanNode(input, GroupByPlanNode(key, km), output)
      input = output
    }

    if (reduce != NullReduce) {
      var output = ReifiedDistCollection(DCUtil.generateNewCollectionURI, kvp)
      ExecutionPlan.addPlanNode(input, new CombinePlanNode(reduce), output)
      input = output
    }

    sgbrResult.result(input.location)
    //    ExecutionPlan.execute(input)
  }


  def distDo(distOp: (T, UntypedEmitter, DistContext) => Unit, outputs: GenSeq[(CollectionId, Manifest[_])]) = {
    val outDistColls = outputs.map(out => new DistColl[Any](out._1.location))
    val node = ExecutionPlan.addPlanNode(List(this), new DistDoPlanNode[T](distOp), outDistColls)

    //    ExecutionPlan.execute(outDistColls)
    outDistColls
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
