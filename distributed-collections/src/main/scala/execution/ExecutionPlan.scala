package execution

import java.util.UUID
import java.net.URI
import scala.collection.distributed.api.dag._
import collection.distributed.api.ReifiedDistCollection
import collection.{GenTraversableOnce, GenSeq}
import collection.mutable.ArrayBuffer

object ExecutionPlan {
  var exPlanDAG: ExPlanDAG = new ExPlanDAG()
  val markedCollections = new ArrayBuffer[ReifiedDistCollection]()

  def addPlanNode(inputs: GenSeq[ReifiedDistCollection], newPlanNode: PlanNode, output: GenSeq[ReifiedDistCollection]): PlanNode = {
    copy(inputs).foreach(input => findOrCreateParent(input).connect(input, newPlanNode))
    output.foreach(v => newPlanNode.outEdges.put(ReifiedDistCollection.copy(v), new ArrayBuffer))

    newPlanNode
  }

  def addPlanNode(input: ReifiedDistCollection, newPlanNode: PlanNode, out: ReifiedDistCollection): PlanNode =
    addPlanNode(List(input), newPlanNode, List(out))

  def markCollection(coll: ReifiedDistCollection) = markedCollections += coll

  def execute: Unit = execute(Nil)

  def execute(outputs: ReifiedDistCollection*): Unit =
    execute(outputs)

  def execute(outputs: GenTraversableOnce[ReifiedDistCollection]): Unit = {

    // attach output nodes
    (Nil ++ outputs ++ markedCollections).foreach(output => exPlanDAG.getPlanNode(output).get.connect(output, new OutputPlanNode(ReifiedDistCollection(output))))
    markedCollections.clear()

    println(ExecutionPlan.toString)

    JobExecutor.execute(exPlanDAG)
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