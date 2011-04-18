package execution

import scala.collection.distributed.DistCollection
import java.util.UUID
import java.net.URI
import scala.collection.distributed.api.dag._
import collection.mutable
import scala.collection.distributed.api.{CollectionId}

/**
 * User: vjovanovic
 * Date: 3/20/11
 */
// TODO (VJ) make thread safe
object ExecutionPlan {
  var exPlanDAG: ExPlanDAG = new ExPlanDAG()
  var globalCache = new mutable.HashMap[String, Any]()

  def addInputCollection(collection: DistCollection[_]): PlanNode = {
    val node = new InputPlanNode(collection)
    exPlanDAG.addInputNode(node)
    node
  }

  def addPlanNode(collection: CollectionId, newPlanNode: PlanNode) = {
    var existingNode = exPlanDAG.getPlanNode(collection)
    if (existingNode.isEmpty) {
      val inputNode = new InputPlanNode(collection)
      existingNode = Some(inputNode)
      exPlanDAG.addInputNode(inputNode)
    }

    existingNode.get.addOutEdge(newPlanNode)
    newPlanNode.addInEdge(existingNode.get)

    newPlanNode
  }

  def addFlattenNode(newFlattenPlanNode: FlattenPlanNode) = {
    newFlattenPlanNode.collections.foreach((collection: CollectionId) => {
      var existingNode = exPlanDAG.getPlanNode(collection)
      if (existingNode.isEmpty) {
        val inputNode = new InputPlanNode(collection)
        existingNode = Some(inputNode)
        exPlanDAG.addInputNode(inputNode)
      }
      newFlattenPlanNode.addInEdge(existingNode.get)
      existingNode.get.addOutEdge(newFlattenPlanNode)
    })
    newFlattenPlanNode
  }

  def sendToOutput(planNode: PlanNode, collection: CollectionId): OutputPlanNode = {
    val outputNode = new OutputPlanNode(collection)

    outputNode.addInEdge(planNode)
    planNode.addOutEdge(outputNode)

    outputNode
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