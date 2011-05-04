package scala.collection.distributed.api.dag

import collection.immutable.GenSeq
import collection.distributed.api.{DistContext, UntypedEmitter, CollectionId}


case class DistDoPlanNode[T](distOP: (T, UntypedEmitter, DistContext) => Unit, outputs: GenSeq[CollectionId]) extends PlanNode
