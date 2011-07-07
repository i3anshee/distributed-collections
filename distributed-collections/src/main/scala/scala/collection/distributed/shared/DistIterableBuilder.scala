package scala.collection.distributed.shared

import java.net.URI
import collection.distributed.{DistIterable, DistCollection}
import collection.distributed.api.shared._
import collection.mutable.{Buffer, ArrayBuffer}
import execution.DCUtil

/**
 * @author Vojin Jovanovic
 */
class DistIterableBuilder[T](uri: URI)
  extends DistSideEffects(CollectionType) with DistIterableBuilderLike[T, DistIterable[T]] {

  def location = uri

  def result(): DistIterable[T] = new DistCollection[T](uri)

  def +=(value: T) = throw new RuntimeException("Addition allowed only in cluster nodes!!!")

}

object DistIterableBuilder {

  def apply[T](): DistIterableBuilderLike[T, DistIterable[T]] = apply(DCUtil.generateNewCollectionURI)

  def apply[T](uri: URI): DistIterableBuilderLike[T, DistIterable[T]] = {
    val proxy = new DistIterableBuilderProxy[T, DistIterable[T]](new DistIterableBuilder(uri))
    DistSideEffects.add(proxy)
    proxy
  }

  def extractBuilders(op: AnyRef): Seq[DistIterableBuilderLike[_, _]] = {
    val buildersBuffer: Buffer[DistIterableBuilderLike[_, _]] = new ArrayBuffer
    op.getClass.getDeclaredFields.foreach(f => {

      if (f.getType.isAssignableFrom(classOf[scala.runtime.ObjectRef])) {
        f.setAccessible(true)
        val elem = f.get(op).asInstanceOf[scala.runtime.ObjectRef].elem;
        if (classOf[DistIterableBuilderLike[_, _]].isAssignableFrom(elem.getClass)) {
          buildersBuffer += elem.asInstanceOf[DistIterableBuilderLike[_, _]]
        }
        f.setAccessible(false)
      } else if (classOf[DistIterableBuilderLike[_, _]].isAssignableFrom(f.getType)) {
        f.setAccessible(true)
        buildersBuffer += f.get(op).asInstanceOf[DistIterableBuilderLike[_, _]]
        f.setAccessible(true)
      }
    })
    buildersBuffer.toSeq
  }

}