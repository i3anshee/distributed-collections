package scala.collection.distributed

import api._
import api.dag._
import api.shared.DistBuilderLike
import collection.generic.GenericCompanion
import scala.colleciton.distributed.hadoop.FSAdapter
import execution.{DCUtil, ExecutionPlan}
import _root_.io.CollectionsIO
import shared.DistIterableBuilder
import collection.{GenIterable, GenTraversable, immutable}

trait DistIterable[+T]
  extends GenIterable[T]
  with GenericDistTemplate[T, DistIterable]
  with DistIterableLike[T, DistIterable[T], Iterable[T]]
  with ReifiedDistCollection {

  //TODO (VJ) move to backend
  def remoteIterable: Iterable[T] = FSAdapter.valuesIterable(this.location)

  override def companion: GenericCompanion[DistIterable] with GenericDistCompanion[DistIterable] = DistIterable

  def stringPrefix = "DistIterable"

  lazy val sizeLongVal: Long = CollectionsIO.getCollectionMetaData(this).size

  def size = if (sizeLong > Int.MaxValue) throw new RuntimeException("Size is larger than MAX_INT!!!") else sizeLong.toInt

  def sizeLong = sizeLongVal

  def nonEmpty = size != 0

  override def isEmpty = sizeLong != 0

  override def toString = seq.mkString(stringPrefix + "(", ", ", ")")

  def isView = false

  protected[this] def flatten[B >: T](collections: GenTraversable[DistIterable[B]]): DistIterable[T] = {
    val outDistColl = new DistCollection[T](DCUtil.generateNewCollectionURI)
    val allCollections = (List(this) ++ collections).map(v => ReifiedDistCollection(v))
    ExecutionPlan.addPlanNode(allCollections, new FlattenPlanNode(allCollections, elemType), List(ReifiedDistCollection(outDistColl)))
    outDistColl
  }

  protected[this] def groupByKey[K, V](kvOp: (T) => (K, V)): DistMap[K, immutable.GenIterable[V]] = {
    val kvp = manifest[Any]
    val output = ReifiedDistCollection(DCUtil.generateNewCollectionURI, kvp)
    ExecutionPlan.addPlanNode(this, GroupByPlanNode(kvOp, kvp), output)
    new DistHashMap[K, immutable.GenIterable[V]](output.location)
  }

  protected[this] def combine[K, V, V1](combine: (Iterable[V]) => V1)(implicit ev: <:<[T, (K, V)]): DistIterable[(K, V1)] = {
    val valRes = manifest[Any]
    val output = ReifiedDistCollection(DCUtil.generateNewCollectionURI, valRes)
    ExecutionPlan.addPlanNode(ReifiedDistCollection(this), CombinePlanNode(combine), output)
    new DistCollection[(K, V1)](output.location)
  }

  protected def distForeach[U](distOp: T => U, builders: scala.Seq[DistBuilderLike[_, _]]) = {
    // extract dist iterable
    val node = ExecutionPlan.addPlanNode(
      List(ReifiedDistCollection(this)), new DistForeachPlanNode[T, U](distOp), builders.map(v => ReifiedDistCollection(v)))
    builders.foreach(_.applyConstraints)
  }

  //TODO (VJ) investigate how to convert to parallel
  protected[this] def parCombiner = throw new UnsupportedOperationException("Not implemented yet!!!")
}

/**$factoryInfo
 */
object DistIterable extends DistFactory[DistIterable] {
  implicit def canBuildFrom[T]: CanDistBuildFrom[Coll, T, DistIterable[T]] = new GenericCanDistBuildFrom[T]

  def newDistBuilder[T] = DistIterableBuilder[T]()

  def newBuilder[T] = DistIterableBuilder[T]()
}
