package scala.collection.distributed.api.dag


case class SortPlanNode[T, S](by: (T) => Ordered[S]) extends PlanNode