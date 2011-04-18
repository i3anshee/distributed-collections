package io

import scala.collection.distributed.api.io.CollectionsIOAPI
import scala.collection.distributed.api.CollectionId

/**
 * User: vjovanovic
 * Date: 4/11/11
 */

object CollectionsIO extends CollectionsIOAPI {
  var collectionsIOStrategy: CollectionsIOAPI = HadoopDFSIO

  def getCollectionMetaData(id: CollectionId) = collectionsIOStrategy.getCollectionMetaData(id)
}