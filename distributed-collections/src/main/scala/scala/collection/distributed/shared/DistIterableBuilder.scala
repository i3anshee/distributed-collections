package scala.collection.distributed.shared

import java.net.URI
import collection.distributed.api.shared._
import collection.mutable.{Buffer, ArrayBuffer}
import collection.distributed._
import execution.{ExecutionPlan, DCUtil}
import scala.Boolean
import reflect.Field

/**
 * @author Vojin Jovanovic
 */

abstract class BuilderDistSideEffects[T, That](uri: URI, val uniqueElements: Boolean = false)
  extends DistSideEffects(CollectionType)
  with DistBuilderLike[T, That] {
  def location = uri

  def +=(value: T) = throw new RuntimeException("Addition allowed only in cluster nodes!!!")
}

//TODO (VJ) LOW maybe implement with higher kinded types
private[this] class DistIterableBuilder[T](uri: URI, uniqueElements: Boolean = false)
  extends BuilderDistSideEffects[T, DistIterable[T]](uri, uniqueElements) {

  def uniqueElementsBuilder = new DistIterableBuilder[T](location, true)

  def result(): DistIterable[T] = new DistCollection[T](location)

  def result(uri: URI): DistIterable[T] = new DistCollection[T](uri)

  def applyConstraints() {}
}

object DistIterableBuilder {

  def apply[T](): DistBuilderLike[T, DistIterable[T]] = apply(DCUtil.generateNewCollectionURI)

  def apply[T](uri: URI): DistBuilderLike[T, DistIterable[T]] = {
    val proxy = new DistBuilderProxy[T, DistIterable[T]](new DistIterableBuilder(uri))
    DistSideEffects.add(proxy)
    proxy
  }

  /**
   * Extracts methods
   */
  def extractBuilders(op: AnyRef): Seq[DistBuilderLike[_, _]] = {
    val buildersBuffer: Buffer[DistBuilderLike[_, _]] = new ArrayBuffer

    op.getClass().getDeclaredFields.foreach(f => {
      println("Type " + f.getType)
      if (f.getType.isAssignableFrom(classOf[scala.runtime.ObjectRef])) {
        // checking vars in closure
        val isAccessible = f.isAccessible
        f.setAccessible(true)

        val elem = f.get(op).asInstanceOf[scala.runtime.ObjectRef].elem
        println("Elem =" + elem.getClass)
        if (classOf[DistBuilderLike[_, _]].isAssignableFrom(elem.getClass))
          buildersBuffer += elem.asInstanceOf[DistBuilderLike[_, _]]

        f.setAccessible(isAccessible)
      } else {
        // checking vals
        val isAccessible = f.isAccessible
        f.setAccessible(true)
        if (classOf[DistBuilderLike[_, _]].isAssignableFrom(f.get(op).getClass)) {
          buildersBuffer += f.get(op).asInstanceOf[DistBuilderLike[_, _]]
        }
        f.setAccessible(isAccessible)
      }
    })
    buildersBuffer.toSeq
  }

}

private[this] class DistCollectionBuilder[T](uri: URI, uniqueElements: Boolean = false)
  extends BuilderDistSideEffects[T, DistCollection[T]](uri, uniqueElements) {

  def uniqueElementsBuilder = new DistCollectionBuilder[T](location, true)

  def result(): DistCollection[T] = new DistCollection[T](location)

  def result(uri: URI): DistCollection[T] = new DistCollection[T](uri)

  def applyConstraints() {}

}

object DistCollectionBuilder {
  def apply[T](): DistBuilderLike[T, DistCollection[T]] = apply(DCUtil.generateNewCollectionURI)

  def apply[T](uri: URI): DistBuilderLike[T, DistCollection[T]] = {
    val proxy = new DistBuilderProxy[T, DistCollection[T]](new DistCollectionBuilder(uri))
    DistSideEffects.add(proxy)
    proxy
  }
}

private[this] class DistCollectionViewBuilder[T](uri: URI, uniqueElements: Boolean = false)
  extends BuilderDistSideEffects[T, DistCollectionView[T]](uri, uniqueElements) {

  def uniqueElementsBuilder = new DistCollectionViewBuilder[T](location, true)

  def result(): DistCollectionView[T] = new DistCollectionView[T](location)

  def result(uri: URI): DistCollectionView[T] = new DistCollectionView[T](uri)

  def applyConstraints = {}

}

object DistCollectionViewBuilder {
  def apply[T](): DistBuilderLike[T, DistCollectionView[T]] = apply(DCUtil.generateNewCollectionURI)

  def apply[T](uri: URI): DistBuilderLike[T, DistCollectionView[T]] = {
    val proxy = new DistBuilderProxy[T, DistCollectionView[T]](new DistCollectionViewBuilder(uri))
    DistSideEffects.add(proxy)
    proxy
  }
}

private[this] class DistSetBuilder[T](uri: URI, uniqueElements: Boolean = false)
  extends BuilderDistSideEffects[T, DistSet[T]](uri, uniqueElements) {

  def uniqueElementsBuilder = new DistSetBuilder[T](location, true)

  def result() = new DistHashSet(location)

  def result(uri: URI) = new DistHashSet(uri)

  def applyConstraints = new DistCollection[T](uri).view.groupBySeq(_.hashCode).flatMap(v => v._2.toSet)
}

object DistSetBuilder {
  def apply[T](): DistBuilderLike[T, DistSet[T]] = apply(DCUtil.generateNewCollectionURI)

  def apply[T](uri: URI): DistBuilderLike[T, DistSet[T]] = {
    val proxy = new DistBuilderProxy[T, DistSet[T]](new DistSetBuilder(uri))
    DistSideEffects.add(proxy)
    proxy
  }
}

private[this] class DistHashSetBuilder[T](uri: URI, uniqueElements: Boolean = false)
  extends BuilderDistSideEffects[T, DistHashSet[T]](uri, uniqueElements) {

  def uniqueElementsBuilder = new DistHashSetBuilder[T](location, true)

  def result() = new DistHashSet[T](location)

  def result(uri: URI) = new DistHashSet[T](uri)

  def applyConstraints = if (!this.uniqueElements) new DistCollection[T](uri).view.groupBySeq(_.hashCode).flatMap(v => v._2.toSet)
}

object DistHashSetBuilder {
  def apply[T](): DistBuilderLike[T, DistHashSet[T]] = apply(DCUtil.generateNewCollectionURI)

  def apply[T](uri: URI): DistBuilderLike[T, DistHashSet[T]] = {
    val proxy = new DistBuilderProxy[T, DistHashSet[T]](new DistHashSetBuilder(uri))
    DistSideEffects.add(proxy)
    proxy
  }
}

private[this] class DistMapBuilder[K, V](uri: URI, uniqueElements: Boolean = false)
  extends BuilderDistSideEffects[(K, V), DistMap[K, V]](uri, uniqueElements) {
  def uniqueElementsBuilder = new DistMapBuilder[K, V](location, true)

  def result() = new DistHashMap[K, V](location)

  def result(uri: URI) = new DistHashMap[K, V](uri)

  def applyConstraints = {}
}

object DistMapBuilder {
  def apply[K, V](): DistBuilderLike[(K, V), DistMap[K, V]] = apply(DCUtil.generateNewCollectionURI)

  def apply[K, V](uri: URI): DistBuilderLike[(K, V), DistMap[K, V]] = {
    val proxy = new DistBuilderProxy[(K, V), DistMap[K, V]](new DistMapBuilder[K, V](uri))
    DistSideEffects.add(proxy)
    proxy
  }
}

private[this] class DistHashMapBuilder[K, V](uri: URI, uniqueElements: Boolean = false)
  extends BuilderDistSideEffects[(K, V), DistHashMap[K, V]](uri, uniqueElements) {

  def uniqueElementsBuilder = new DistHashMapBuilder[K, V](location, true)

  //TODO (VJ) apply map constraints
  def applyConstraints = {}

  def result() = new DistHashMap[K, V](location)

  def result(uri: URI) = new DistHashMap[K, V](uri)
}

object DistHashMapBuilder {
  def apply[K, V](): DistBuilderLike[(K, V), DistHashMap[K, V]] = apply(DCUtil.generateNewCollectionURI)

  def apply[K, V](uri: URI): DistBuilderLike[(K, V), DistHashMap[K, V]] = {
    val proxy = new DistBuilderProxy[(K, V), DistHashMap[K, V]](new DistHashMapBuilder[K, V](uri))
    DistSideEffects.add(proxy)
    proxy
  }
}
