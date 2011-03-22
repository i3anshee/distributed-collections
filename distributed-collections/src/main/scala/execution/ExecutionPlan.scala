package execution

import dag._
import dcollections.DistributedCollection
import java.util.UUID
import java.net.URI
import mrapi.{ClosureMapperAdapter, JobAdapter}

/**
 * User: vjovanovic
 * Date: 3/20/11
 */

object ExecutionPlan {

  // may be superfluous
  var inputs = Set[DistributedCollection]()
  var outputs = Set[DistributedCollection]()
  var intermediateResults = Set[Set[DistributedCollection]]()

  var inputNodes = Set[InputPlanNode]()
  var outputNodes = Set[OutputPlanNode]()

  def addInputCollection(collection: DistributedCollection): PlanNode = {
    inputs += collection
    val node = new InputPlanNode(collection.location)
    inputNodes += node
    node
  }

  def addOperation(planNode: PlanNode, newPlanNode: PlanNode): PlanNode = {
    planNode.addOutEdge(newPlanNode)
    newPlanNode.addInEdge(planNode)
  }

  def sendToOutput(planNode: PlanNode, outputFile: URI): OutputPlanNode = {
    val outputNode = new OutputPlanNode(outputFile);
    outputNode.addInEdge(planNode)
    planNode.addOutEdge(outputNode)
    outputNodes += outputNode
    outputNode
  }

  def execute(): Unit = {
    // TODO (VJ) Here we will optimize the execution plan DAG
    // then we generate one map one reduce execute them until we consume the whole EP DAG

    // for single collection this will do
    val inputNode = inputNodes.last // for now
    val mapperNode = inputNode.outEdges.last
    val reducerNode = mapperNode.outEdges.last
    val outputNode = outputNodes.last // for now

    // we execute all the commands from the EP DAG, but for now just one single input and single output
    JobAdapter.execute(inputNode.inputURI,
      mapperNode.asInstanceOf[MapPlanNode].mapAdapter, reducerNode.asInstanceOf[ReducePlanNode].reduceAdapter,
      outputNode.outputURI)
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
