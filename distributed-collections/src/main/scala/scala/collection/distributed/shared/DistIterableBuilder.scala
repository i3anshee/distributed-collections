package scala.collection.distributed.shared

import java.net.URI
import collection.distributed.api.shared._
import collection.mutable.{Buffer, ArrayBuffer}
import collection.distributed._
import execution.{ExecutionPlan, DCUtil}
import scala.Boolean

/**
 * @author Vojin Jovanovic
 */

private[this] class DistIterableBuilder[T](uri: URI)
  extends DistSideEffects(CollectionType) with DistBuilderLike[T, DistIterable[T]] {

  def location = uri

  def result(): DistIterable[T] = new DistCollection[T](location)

  def +=(value: T) = throw new RuntimeException("Addition allowed only in cluster nodes!!!")

  def applyConstraints = {}

}

object DistIterableBuilder {

  def apply[T](): DistBuilderLike[T, DistIterable[T]] = apply(DCUtil.generateNewCollectionURI)

  def apply[T](uri: URI): DistBuilderLike[T, DistIterable[T]] = {
    val proxy = new DistBuilderProxy[T, DistIterable[T]](new DistIterableBuilder(uri))
    DistSideEffects.add(proxy)
    proxy
  }

  def extractBuilders(op: AnyRef): Seq[DistBuilderLike[_, _]] = {
    val buildersBuffer: Buffer[DistBuilderLike[_, _]] = new ArrayBuffer
    op.getClass.getDeclaredFields.foreach(f => {

      if (f.getType.isAssignableFrom(classOf[scala.runtime.ObjectRef])) {
        f.setAccessible(true)
        val elem = f.get(op).asInstanceOf[scala.runtime.ObjectRef].elem;
        if (classOf[DistBuilderLike[_, _]].isAssignableFrom(elem.getClass)) {
          buildersBuffer += elem.asInstanceOf[DistBuilderLike[_, _]]
        }
        f.setAccessible(false)
      } else if (classOf[DistBuilderLike[_, _]].isAssignableFrom(f.getType)) {
        f.setAccessible(true)
        buildersBuffer += f.get(op).asInstanceOf[DistBuilderLike[_, _]]
        f.setAccessible(true)
      }
    })
    buildersBuffer.toSeq
  }

}

private[this] class DistCollectionBuilder[T](uri: URI) extends DistSideEffects(CollectionType) with DistBuilderLike[T, DistCollection[T]] {

  def location = uri

  def result(): DistCollection[T] = new DistCollection[T](location)

  def +=(value: T) = throw new RuntimeException("Addition allowed only in cluster nodes!!!")

  def applyConstraints = {}

}

object DistCollectionBuilder {
  def apply[T](): DistBuilderLike[T, DistCollection[T]] = apply(DCUtil.generateNewCollectionURI)

  def apply[T](uri: URI): DistBuilderLike[T, DistCollection[T]] = {
    val proxy = new DistBuilderProxy[T, DistCollection[T]](new DistCollectionBuilder(uri))
    DistSideEffects.add(proxy)
    proxy
  }
}

private[this] class DistSetBuilder[T](uri: URI)
  extends DistSideEffects(CollectionType) with DistBuilderLike[T, DistSet[T]] {
  def location = uri

  def result() = new DistHashSet(uri)

  def +=(value: T) = throw new UnsupportedOperationException("Element addition allowed only in cluster nodes!!!")

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

private[this] class DistHashSetBuilder[T](uri: URI) extends DistSideEffects(CollectionType) with DistBuilderLike[T, DistHashSet[T]] {
  def location = uri

  def result() = new DistHashSet(uri)

  def +=(value: T) = throw new UnsupportedOperationException("Element addition allowed only in cluster nodes!!!")

  def applyConstraints = new DistCollection[T](uri).view.groupBySeq(_.hashCode).flatMap(v => v._2.toSet)
}

object DistHashSetBuilder {
  def apply[T](): DistBuilderLike[T, DistHashSet[T]] = apply(DCUtil.generateNewCollectionURI)

  def apply[T](uri: URI): DistBuilderLike[T, DistHashSet[T]] = {
    val proxy = new DistBuilderProxy[T, DistHashSet[T]](new DistHashSetBuilder(uri))
    DistSideEffects.add(proxy)
    proxy
  }
}

private[this] class DistMapBuilder[K, V](uri: URI) extends DistSideEffects(CollectionType) with DistBuilderLike[(K, V), DistMap[K, V]] {
  def location = uri

  def +=(elem: (K, V)) = throw new UnsupportedOperationException("Element addition allowed only in cluster nodes!!!")

  def applyConstraints = {}

  def result() = new DistHashMap[K, V](location)
}

object DistMapBuilder {
  def apply[K, V](): DistBuilderLike[(K, V), DistMap[K, V]] = apply(DCUtil.generateNewCollectionURI)

  def apply[K, V](uri: URI): DistBuilderLike[(K, V), DistMap[K, V]] = {
    val proxy = new DistBuilderProxy[(K, V), DistMap[K, V]](new DistMapBuilder[K, V](uri))
    DistSideEffects.add(proxy)
    proxy
  }
}

private[this] class DistHashMapBuilder[K, V](uri: URI) extends DistSideEffects(CollectionType) with DistBuilderLike[(K, V), DistHashMap[K, V]] {
  def location = uri

  def +=(elem: (K, V)) = throw new UnsupportedOperationException("Element addition allowed only in cluster nodes!!!")

  def applyConstraints = {}

  def result() = new DistHashMap[K, V](location)
}

object DistHashMapBuilder {
  def apply[K, V](): DistBuilderLike[(K, V), DistHashMap[K, V]] = apply(DCUtil.generateNewCollectionURI)

  def apply[K, V](uri: URI): DistBuilderLike[(K, V), DistHashMap[K, V]] = {
    val proxy = new DistBuilderProxy[(K, V), DistHashMap[K, V]](new DistHashMapBuilder[K, V](uri))
    DistSideEffects.add(proxy)
    proxy
  }
}
