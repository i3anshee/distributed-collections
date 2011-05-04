package execution

import scala.collection.distributed.DistCollection
import java.util.UUID
import java.net.URI
import scala.collection.distributed.api.dag._
import collection.distributed.api.{UniqueId, CollectionId}
import collection.{GenSeq, mutable}

// TODO (VJ) make thread safe
object ExecutionPlan {
  var exPlanDAG: ExPlanDAG = new ExPlanDAG()
  var globalCache = new mutable.HashMap[String, Any]()

  def addPlanNode(collectionId: CollectionId, newPlanNode: PlanNode, out: GenSeq[CollectionId]): PlanNode = {
    // find output node
    var existingNode = exPlanDAG.getPlanNode(collectionId)
    if (existingNode.isEmpty) {
      val inputNode = new InputPlanNode(collectionId.location)
      existingNode = Some(inputNode)
      exPlanDAG.addInputNode(inputNode)
    } else {
      existingNode.get.disconnect()
    }

    existingNode.get.connect(newPlanNode, collectionId)
    out.foreach(outId => {
      newPlanNode.connect(new OutputPlanNode(outId.location), outId)
    })
    newPlanNode
  }

  def addPlanNode(collectionId: CollectionId, newPlanNode: PlanNode, out: CollectionId): PlanNode =
    addPlanNode(collectionId, newPlanNode, List(out))

  def addFlattenNode(newFlattenPlanNode: FlattenPlanNode, outId: CollectionId) = {
    newFlattenPlanNode.collections.foreach((collection: CollectionId) => {
      var existingNode = exPlanDAG.getPlanNode(collection)
      if (existingNode.isEmpty) {
        val inputNode = new InputPlanNode(collection.location)
        existingNode = Some(inputNode)
        exPlanDAG.addInputNode(inputNode)
      } else
        existingNode.get.disconnect()

      existingNode.get.connect(newFlattenPlanNode, collection)
      newFlattenPlanNode.connect(new OutputPlanNode(outId.location), outId)
    })

    newFlattenPlanNode
  }

  def execute(): Unit = {
    JobExecutor.execute(exPlanDAG, globalCache)
    exPlanDAG = new ExPlanDAG()
  }

}

/**
 * This object contains all the methods that have not been placed in the software design but are certainly needed.
 */
object DCUtil {

  // TODO (VJ) this needs to be set by the framework and if any management needs to be done it is orthogonal to this project
  var baseURIString = "dcollections/"

  /**
   * Generates a new collection file system identifier.
   */
  def generateNewCollectionURI() = new URI(baseURIString + UUID.randomUUID().toString)

}