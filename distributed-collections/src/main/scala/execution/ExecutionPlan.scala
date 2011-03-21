package execution

import dag.{MapPlanNode, OutputPlanNode, PlanNode, InputPlanNode}
import dcollections.DistributedCollection
import java.util.UUID
import java.net.URI
import mrapi.{ClosureMapperAdapter, JobAdapter}

/**
 * User: vjovanovic
 * Date: 3/20/11
 */

object ExecutionPlan {

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

  def addMapOperation[A, B](planNode: PlanNode, closure: (A) => B): PlanNode = {
    val mapNode = new MapPlanNode(closure)
    mapNode.addInEdge(planNode)
    planNode.addOutEdge(mapNode)
    mapNode
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
    val inputNode = inputNodes.last // for now
    val mapperNode = inputNode.outEdges.last
    val outputNode = outputNodes.last // for now

    // we execute all the commands from the EP DAG, but for now just one single input and single output
    JobAdapter.execute(inputNode.inputURI, new ClosureMapperAdapter(
      mapperNode.asInstanceOf[MapPlanNode[AnyRef, AnyRef]].closure), null, outputNode.outputURI)
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
