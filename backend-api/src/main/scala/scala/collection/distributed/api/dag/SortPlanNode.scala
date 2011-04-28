package scala.collection.distributed.api.dag

import collection.distributed.api.CollectionId

final class SortPlanNode[T, S](override val id: CollectionId, by: (T) => Ordered[S]) extends PlanNode(id)