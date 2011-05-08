package execution

import java.util.UUID
import java.net.URI
import scala.collection.distributed.api.dag._
import collection.distributed.api.{ReifiedDistCollection}
import collection.{GenTraversable, GenSeq, mutable}
import mutable.ArrayBuffer

object ExecutionPlan {
  var exPlanDAG: ExPlanDAG = new ExPlanDAG()
  var globalCache = new mutable.HashMap[String, Any]()

  def addPlanNode(inputs: GenSeq[ReifiedDistCollection], newPlanNode: PlanNode, output: GenSeq[ReifiedDistCollection]): PlanNode = {
    copy(inputs).foreach(input => findOrCreateParent(input).connect(input, newPlanNode))
    output.foreach(v => newPlanNode.outEdges.put(ReifiedDistCollection.copy(v), new ArrayBuffer))

    newPlanNode
  }

  def addPlanNode(input: ReifiedDistCollection, newPlanNode: PlanNode, out: ReifiedDistCollection): PlanNode =
    addPlanNode(List(input), newPlanNode, List(out))

  def execute(outputs: ReifiedDistCollection*): Unit =
    execute(outputs)

  def execute(outputs: GenTraversable[ReifiedDistCollection]): Unit = {

    // attach outputs that need to be saved
    outputs.foreach(output => {
      exPlanDAG.getPlanNode(output).get.connect(output, new OutputPlanNode(output))
    })

    println(ExecutionPlan.toString)

    JobExecutor.execute(exPlanDAG, globalCache)
    exPlanDAG = new ExPlanDAG()
  }

  private[this] def findOrCreateParent(input: ReifiedDistCollection): PlanNode = {
    var existingNode = exPlanDAG.getPlanNode(input)

    // if there is no node add input
    if (existingNode.isEmpty) {
      val inputNode = new InputPlanNode(input)
      existingNode = Some(inputNode)
      exPlanDAG.addInputNode(inputNode)
    }

    existingNode.get
  }

  private[this] def copy(input: GenSeq[ReifiedDistCollection]): GenSeq[ReifiedDistCollection] = input.map(v => ReifiedDistCollection.copy(v))

  override def toString = exPlanDAG.toString
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