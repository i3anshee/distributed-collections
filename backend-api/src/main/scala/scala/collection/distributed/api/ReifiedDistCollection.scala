package scala.collection.distributed.api

import java.net.URI

/**
 * @author Vojin Jovanovic
 */

trait ReifiedDistCollection extends CollectionId {

  def elemType: Manifest[_] = manifest[Any]

  override def toString = "(" + location + ", " + elemType.toString + ")"

}

object ReifiedDistCollection {

  def apply(collection: ReifiedDistCollection): ReifiedDistCollection =
    apply(collection.location, collection.elemType)

  def apply(uri: URI, manifest: Manifest[_]) = new ReifiedDistCollection {
    def location = uri

    override def elemType = manifest
  }

}