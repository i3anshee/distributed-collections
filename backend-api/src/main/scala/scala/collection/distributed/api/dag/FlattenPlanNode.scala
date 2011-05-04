package scala.collection.distributed.api.dag

import collection.distributed.api.{CollectionId}

case class FlattenPlanNode(collections: Traversable[CollectionId]) extends PlanNode