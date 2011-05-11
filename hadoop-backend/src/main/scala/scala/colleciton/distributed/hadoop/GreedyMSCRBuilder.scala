package scala.colleciton.distributed.hadoop

import collection.distributed.api.dag._

import collection.mutable
import mutable.{ArrayBuffer, Buffer, HashMap}
import org.apache.hadoop.io.{NullWritable, BytesWritable}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileOutputFormat, SequenceFileInputFormat, JobConf, FileInputFormat}
import scala.util.Random
import java.net.URI
import java.util.UUID

class GreedyMSCRBuilder extends JobBuilder {

  val mapDAG: ExPlanDAG = new ExPlanDAG()
  val reduceDAG: ExPlanDAG = new ExPlanDAG()

  val tempFileToURI: mutable.Map[String, URI] = new HashMap
  val outputDir = new Path("./tmp/" + Math.abs(Random.nextInt))

  // add input to combine node
  val combinedInputs: Buffer[URI] = new ArrayBuffer
  val intermediateOutputs: Buffer[URI] = new ArrayBuffer

  def build(dag: ExPlanDAG) = {

    val matches = (n: PlanNode) => n match {
      case v: GroupByPlanNode[_, _, _] => (true, false)
      case v: CombinePlanNode[_, _, _, _] => throw new RuntimeException("Should never be reached !!!")
      case _ => (true, true)
    }

    // collect mapper phase (stopping after groupBy)
    val startingNodes = new mutable.Queue() ++ dag.inputNodes
    val visited = new mutable.HashSet[PlanNode]

    startingNodes.foreach((node: PlanNode) => buildDag(node, mapDAG, visited, matches))

    val refinedDAG = buildRemainingDAG(dag, visited)

    visited.clear
    startingNodes.clear
    startingNodes ++= refinedDAG.inputNodes

    val matchesReducer = (n: PlanNode) => n match {
      case v: DistDoPlanNode[_] => (true, true)
      case v: OutputPlanNode => (true, false)
      case v: CombinePlanNode[_, _, _, _] => (true, true)
      case _ => (false, false)
    }

    startingNodes.foreach((node: PlanNode) => buildDag(node, reduceDAG, visited, matches))

    // temporary fields for outputs
    val outputURIs = outputNodes(reduceDAG).map(_.collection.location) ++ (outputNodes(mapDAG).map(_.collection.location).toSet -- reduceDAG.inputNodes.map(_.collection.location))
    val intermediateURIs = outputNodes(mapDAG).map(_.collection.location).toSet.intersect(reduceDAG.inputNodes.map(_.collection.location))

    val combinedInputs = reduceDAG.filter(_ match {
      case v: CombinePlanNode[_, _, _, _] => true
      case _ => false
    }).map(_.inEdges.head.asInstanceOf[InputPlanNode].collection.location)

    // if mapper has an output node that is matched by reduce than there is not temporary file (and the collector is set to default)
    println("Output")
    outputURIs.foreach(v => println(v.toString))
    println("Intermediate")
    intermediateURIs.foreach(v => println(v.toString))
    println("Combined")
    combinedInputs.foreach(v => println(v.toString))

    this.combinedInputs ++= combinedInputs
    this.intermediateOutputs ++= intermediateURIs
    outputURIs.foreach(v => this.tempFileToURI.put(getTempFileName, v))


    buildRemainingDAG(refinedDAG, visited)
  }

  def configure(job: JobConf) =
    if (!mapDAG.isEmpty) {
      HadoopJob.dfsSerialize(job, "distribted-collections.mapDAG", mapDAG)
      HadoopJob.dfsSerialize(job, "distribted-collections.intermediateOutputs", intermediateOutputs)
      HadoopJob.dfsSerialize(job, "distribted-collections.tempFileToURI", tempFileToURI)

      if (!reduceDAG.isEmpty) {
        HadoopJob.dfsSerialize(job, "distribted-collections.reduceDAG", reduceDAG)
      }

      if (!combinedInputs.isEmpty) {
        // serialize combined inputs
        HadoopJob.dfsSerialize(job, "distribted-collections.combinedInputs", combinedInputs)
        // set a job class
      }

      // setting input and output and intermediate types
      job.setMapOutputKeyClass(classOf[BytesWritable])
      job.setMapOutputValueClass(classOf[BytesWritable])
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[BytesWritable])

      // set the input and output files for the job
      mapDAG.inputNodes.foreach((in) => FileInputFormat.addInputPath(job, new Path(in.collection.location.toString)))
      FileInputFormat.setInputPathFilter(job, classOf[MetaPathFilter])
      FileOutputFormat.setOutputPath(job, outputDir)

      QuickTypeFixScalaI0.setJobClassesBecause210SnapshotWillNot(job, false, !reduceDAG.isEmpty, tempFileToURI.keys.toArray)
    }


  def postRun(job: JobConf) = {
    // rename mapper files to part files
//    tempFileToURI.foreach(v => )

    //tempFileToURI.foreach(v => FSAdapter.rename(job, new OutputtempFileToURI))
  }

  def outputNodes(dag: ExPlanDAG) = dag.flatMap(_ match {
    case v: OutputPlanNode => List(v)
    case _ => List[OutputPlanNode]()
  })

  def outputNodes: Traversable[OutputPlanNode] = outputNodes(if (reduceDAG.isEmpty) mapDAG else reduceDAG)


  def outputs = outputNodes.map(_.collection)

  def feasible(node: PlanNode, matches: (PlanNode) => (Boolean, Boolean), visited: mutable.Set[PlanNode]): Boolean = {
    val (take, continue) = matches(node)
    if (take && continue)
      if (visited.contains(node))
        true
      else
        node.predecessors.forall(pred => feasible(pred, matches, visited))
    else
      false
  }

  def buildRemainingDAG(dag: ExPlanDAG, visited: mutable.Set[PlanNode]): ExPlanDAG = {
    val refinedDAG = new ExPlanDAG()
    dag.foreach(node => {
      if (!visited.contains(node)) {
        val nodeCopy = node.copyUnconnected()

        node.inEdges.foreach(edge => if (visited.contains(edge._1)) {
          var existing = refinedDAG.getPlanNode(edge._2)
          if (!existing.isDefined) {
            val newInput = new InputPlanNode(edge._2)
            refinedDAG.addInputNode(newInput)
            existing = Some(newInput)
          }

          existing.get.connect(edge._2, nodeCopy)
        } else {
          refinedDAG.getPlanNode(edge._1).get.connect(edge._2, nodeCopy)
        })

        refinedDAG.connectDownwards(nodeCopy, node)
      }
    })
    refinedDAG
  }

  def buildDag(node: PlanNode, newDAG: ExPlanDAG, visited: mutable.Set[PlanNode], matches: (PlanNode) => (Boolean, Boolean)): Unit = {
    val (take, continue) = matches(node)

    if (take && node.predecessors.forall(v => feasible(v, matches, visited))) {
      visited += node
      node match {
        case v: InputPlanNode =>
          val copiedNode = v.copyUnconnected
          newDAG.addInputNode(copiedNode)

          newDAG.connectDownwards(copiedNode, v)
        case _ => // copy the node to newDAG with all connections
          newDAG.connect(node.copyUnconnected, node)
      }

      if (continue)
        node.children.foreach(node => {
          if (!visited.contains(node))
            buildDag(node, newDAG, visited, matches)
        })
      else
        node.outEdges.foreach(edge => edge._2.foreach(child => newDAG.getPlanNode(node).get.connect(
          edge._1, new OutputPlanNode(edge._1))))

    } else {
      // add an output node to newDAG if not already present
      node.inEdges.foreach(edge => {
        val parent = newDAG.getPlanNode(edge._1)
        if (parent.isDefined) {
          val parentOutputs = parent.get.outEdges.get(edge._2)
          if (!(parentOutputs.isDefined && parentOutputs.get.exists(_ match {
            case v: OutputPlanNode =>
              true
            case _ =>
              false
          })))
            parent.get.connect(edge._2, new OutputPlanNode(edge._2))
        }
      })
    }
  }

  private[this] def getTempFileName = UUID.randomUUID.toString.replaceAll("-", "")
}