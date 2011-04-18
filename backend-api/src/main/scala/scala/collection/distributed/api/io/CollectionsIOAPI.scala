package scala.collection.distributed.api.io

import scala.collection.distributed.api.CollectionId

/**
 * User: vjovanovic
 * Date: 4/11/11
 */

trait CollectionsIOAPI {

  def getCollectionMetaData(id: CollectionId): CollectionMetaData

}