package scala.colleciton.distributed.hadoop

import collection.distributed.api.dag._
import org.apache.hadoop.mapreduce.Job
import collection.mutable
import org.apache.hadoop.io.{NullWritable, BytesWritable}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

class GreedyMSCRBuilder extends MSCRBuilder {
  // new builder interface
  val mapperDAG: ExPlanDAG = new ExPlanDAG()
  val reduceDAG: ExPlanDAG = new ExPlanDAG()

  def build(dag: ExPlanDAG) = {

    val matches = (n: PlanNode) => n match {
      case v: GroupByPlanNode[_, _, _] => (true, false)
      case v: OutputPlanNode => (false, false)
      case v: CombinePlanNode[_, _, _, _] => throw new RuntimeException("Should never be reached !!!")
      case _ => (true, true)
    }

    // collect mapper phase (stopping after groupBy and flatten)
    val startingNodes = new mutable.Queue() ++ dag.inputNodes
    val visited = new mutable.HashSet[PlanNode]

    startingNodes.foreach((node: PlanNode) => buildDag(node, mapperDAG, visited, matches))

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

    buildRemainingDAG(refinedDAG, visited)
  }


  def configure(job: Job) =
    if (!mapperDAG.isEmpty) {
      HadoopJob.dfsSerialize(job, "distcoll.mapperDAG", mapperDAG)

      if (!reduceDAG.isEmpty) {
        HadoopJob.dfsSerialize(job, "distcoll.reduceDAG", reduceDAG)
      }

      // setting input and output and intermediate types
      job.setMapOutputKeyClass(classOf[BytesWritable])
      job.setMapOutputValueClass(classOf[BytesWritable])
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[BytesWritable])

      // set the input and output files for the job
      mapperDAG.inputNodes.foreach((in) => FileInputFormat.addInputPath(job, new Path(in.collection.location.toString)))
      FileInputFormat.setInputPathFilter(job, classOf[MetaPathFilter])

      val outputDAG = if (reduceDAG.isEmpty) mapperDAG else reduceDAG
      val outputNodes = outputDAG.flatMap(_ match {
        case v: OutputPlanNode => List(v)
        case _ => List[OutputPlanNode]()
      })
      QuickTypeFixScalaI0.setJobClassesBecause210SnapshotWillNot(job, false, !reduceDAG.isEmpty, outputNodes.toArray)
    }


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

  def connectDownwards(node: PlanNode, newDAG: ExPlanDAG, copiedNode: PlanNode) =
    node.outEdges.foreach(outputs => {
      outputs._2.foreach(node => {
        val child = newDAG.getPlanNode(node)
        if (child.isDefined)
          copiedNode.connect(outputs._1, child.get)
      })
    })

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

        connectDownwards(node, refinedDAG, nodeCopy)
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

          connectDownwards(v, newDAG, copiedNode)
        case _ => // copy the node to newDAG with all connections
          val copiedNode = node.copyUnconnected

          // connect upwards
          node.inEdges.foreach(node => {
            val parent = newDAG.getPlanNode(node._1)
            if (parent.isDefined)
              parent.get.connect(node._2, copiedNode)
          })

          connectDownwards(node, newDAG, copiedNode)
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
}