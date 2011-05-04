package scala.collection.distributed.api.dag

import collection.distributed.api.{Emitter}

case class GroupByPlanNode[A, B, K](keyFunction: (A, Emitter[B]) => K) extends PlanNode