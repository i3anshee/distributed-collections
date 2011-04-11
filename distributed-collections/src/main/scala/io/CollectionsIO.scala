package io

import dcollections.api.io.CollectionsIOAPI
import dcollections.api.CollectionId

/**
 * User: vjovanovic
 * Date: 4/11/11
 */

object CollectionsIO extends CollectionsIOAPI {
  var collectionsIOStrategy: CollectionsIOAPI = HadoopDFSIO

  def getCollectionMetaData(id: CollectionId) = collectionsIOStrategy.getCollectionMetaData(id)
}