package dcollections.api.dag

import dcollections.api.CollectionId

/**
 * User: vjovanovic
 * Date: 4/5/11
 */

class FlattenPlanNode(override val id: CollectionId, val collections: Traversable[CollectionId]) extends PlanNode(id) {
}