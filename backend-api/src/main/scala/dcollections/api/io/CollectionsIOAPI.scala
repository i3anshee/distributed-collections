package dcollections.api.io

import dcollections.api.CollectionId

/**
 * User: vjovanovic
 * Date: 4/11/11
 */

trait CollectionsIOAPI {

  def getCollectionMetaData(id: CollectionId): CollectionMetaData

}