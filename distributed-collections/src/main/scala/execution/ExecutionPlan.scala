package execution

import dcollections.DistCollection
import java.util.UUID
import java.net.URI
import dcollections.api.dag.{OutputPlanNode, InputPlanNode, PlanNode, ExPlanDAG}

/**
 * User: vjovanovic
 * Date: 3/20/11
 */
// TODO (VJ) make thread safe
object ExecutionPlan {
  var exPlanDAG: ExPlanDAG = new ExPlanDAG()

  def addInputCollection(collection: DistCollection[_]): PlanNode = {
    val node = new InputPlanNode(collection.location)
    exPlanDAG.addInputNode(node)
    node
  }

  def addPlanNode(collection: DistCollection[_], newPlanNode: PlanNode) = {
    var existingNode = exPlanDAG.getPlanNode(collection.uID)
    if (existingNode.isEmpty) {
      val inputNode = new InputPlanNode(collection.location)
      existingNode = Some(inputNode)
      exPlanDAG.addInputNode(inputNode)
    }

    existingNode.get.addOutEdge(newPlanNode)
    newPlanNode.addInEdge(existingNode.get)

    newPlanNode
  }

  def addFlattenNode(collection: DistCollection[_], newFlattenPlanNode: PlanNode) = {
    // check for every collection
    throw new UnsupportedOperationException("Yet to be implemented!")
  }

  def sendToOutput(planNode: PlanNode, outputFile: URI): OutputPlanNode = {
    val outputNode = new OutputPlanNode(outputFile);

    outputNode.addInEdge(planNode)
    planNode.addOutEdge(outputNode)

    outputNode
  }

  def execute(): Unit = {
    JobExecutor.execute(exPlanDAG)
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