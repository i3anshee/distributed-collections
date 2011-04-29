package scala.collection.distributed.api.dag

import collection.distributed.api.{IndexedEmitter, DistContext, CollectionId}
import collection.immutable.GenSeq

/**
 * User: vjovanovic
 * Date: 4/29/11
 */

class DistDoPlanNode[T](id: CollectionId, val distOP: (T, IndexedEmitter, DistContext) => Unit, val outputs: GenSeq[CollectionId])
extends PlanNode(id) {

}
