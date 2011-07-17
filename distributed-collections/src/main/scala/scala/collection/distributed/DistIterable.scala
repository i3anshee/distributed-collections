package scala.collection.distributed

import api._
import api.dag._
import api.shared.DistBuilderLike
import collection.generic.GenericCompanion
import scala.colleciton.distributed.hadoop.FSAdapter
import execution.{DCUtil, ExecutionPlan}
import _root_.io.CollectionsIO
import collection.{GenTraversable, immutable}
import shared.DistIterableBuilder

trait DistIterable[+T]
  extends immutable.GenIterable[T]
  with GenericDistTemplate[T, DistIterable]
  with DistIterableLike[T, DistIterable[T], immutable.Iterable[T]]
  with ReifiedDistCollection {

  def remoteIterable: immutable.Iterable[T] = FSAdapter.valuesIterable(this.location)

  override def companion: GenericCompanion[DistIterable] with GenericDistCompanion[DistIterable] = DistIterable

  def stringPrefix = "DistIterable"

  lazy val sizeLongVal: Long = CollectionsIO.getCollectionMetaData(this).size

  def size = if (sizeLong > Int.MaxValue) throw new RuntimeException("Size is larger than MAX_INT!!!") else sizeLong.toInt

  def sizeLong = sizeLongVal

  def nonEmpty = size != 0

  override def isEmpty = sizeLong != 0

  override def toString = seq.mkString(stringPrefix + "(", ", ", ")")

  def isView = false

  def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T] = {
    val outDistColl = new DistCollection[T](DCUtil.generateNewCollectionURI)
    val allCollections = List(this) ++ collections
    ExecutionPlan.addPlanNode(allCollections, new FlattenPlanNode(allCollections, elemType), List(outDistColl))
    outDistColl
  }

  def groupBySort[S, K, K1 <: K, T1](key: (T, Emitter[T1]) => K, by: (K1) => Ordered[S] = nullOrdered[K]): DistMap[K, immutable.GenIterable[T1]] with DistCombinable[K, T1] = {
    val km = manifest[Any]
    val sm = manifest[Any]
    val t1m = manifest[Any]
    val t2m = manifest[Any]
    val kvp = manifest[(Any, Any)]


    var input: ReifiedDistCollection = this

    var output = ReifiedDistCollection(DCUtil.generateNewCollectionURI, kvp)
    ExecutionPlan.addPlanNode(input, GroupByPlanNode(key, km), output)
    input = output

    if (by != NullOrdered) {
      var output = ReifiedDistCollection(DCUtil.generateNewCollectionURI, elemType)
      ExecutionPlan.addPlanNode(input, new SortPlanNode[K1, S](by, sm), output)
      input = output
    }

    new DistHashMap[K, immutable.GenIterable[T1]](input.location) with DistCombinable[K, T1] {
      def combine[T2 >: T1](combine: (Iterable[T1]) => T2) = {
        var output = ReifiedDistCollection(DCUtil.generateNewCollectionURI, kvp)
        ExecutionPlan.addPlanNode(input, new CombinePlanNode(combine), output)
        new DistHashMap[K, T2](output.location)
      }
    }
  }

  protected def distForeach[U](distOp: T => U, builders: scala.Seq[DistBuilderLike[_, _]]) {
    // extract dist iterable
    val node = ExecutionPlan.addPlanNode(List(this), new DistForeachPlanNode[T, U](distOp), builders.map(v => ReifiedDistCollection(v)))
    builders.foreach(_.applyConstraints)
  }

  def distDo(distOp: (T, UntypedEmitter, DistContext) => Unit, outputs: immutable.GenSeq[(CollectionId, Manifest[_])]) = {
    val outDistColls = outputs.map(out => new DistCollection[Any](out._1.location))
    val node = ExecutionPlan.addPlanNode(List(this), new DistDoPlanNode[T](distOp), outDistColls)

    outDistColls
  }

  //TODO (VJ) investigate what is the problem
  protected[this] def parCombiner = throw new UnsupportedOperationException("Not implemented yet!!!")

  def delete: Unit = null
}

/**$factoryInfo
 */
object DistIterable extends DistFactory[DistIterable] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistIterable[T]] = new GenericCanDistBuildFrom[T]

  def newDistBuilder[T] = DistIterableBuilder[T]()

  def newBuilder[T] = DistIterableBuilder[T]()
}
