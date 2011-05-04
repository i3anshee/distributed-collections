package scala.collection.distributed.api.dag


case class CombinePlanNode[A, K, B, C](op: (C, B) => C) extends PlanNode