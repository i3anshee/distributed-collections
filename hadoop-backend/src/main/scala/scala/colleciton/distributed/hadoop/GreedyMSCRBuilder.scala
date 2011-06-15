package scala.colleciton.distributed.hadoop

import collection.distributed.api.dag._

import collection.mutable
import mutable.{ArrayBuffer, Buffer, HashMap}
import org.apache.hadoop.io.{NullWritable, BytesWritable}
import scala.util.Random
import java.net.URI
import java.util.UUID
import org.apache.hadoop.mapred._
import lib.MultipleOutputs
import scala.Boolean
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import collection.distributed.api.io.CollectionMetaData
import collection.JavaConversions._

class GreedyMSCRBuilder extends JobBuilder {

  val mapDAG: ExPlanDAG = new ExPlanDAG()
  val reduceDAG: ExPlanDAG = new ExPlanDAG()

  val tempFileToURI: mutable.Map[String, URI] = new HashMap
  val outputDir = new Path("./tmp/" + Math.abs(Random.nextInt))

  // add input to combine node
  val combinedInputs: mutable.Map[URI, CombinePlanNode[_, _]] = new mutable.HashMap
  val intermediateOutputs: Buffer[URI] = new ArrayBuffer
  val intermediateToByte: mutable.Map[URI, Byte] = new mutable.HashMap
  val toClean: Buffer[URI] = new ArrayBuffer

  def build(dag: ExPlanDAG) = {

    val matches = (n: PlanNode) => n match {
      case v: GroupByPlanNode[_, _, _] => (true, false)
      case v: CombinePlanNode[_, _] => (false, false)
      case _ => (true, true)
    }

    // collect mapper phase (stopping after groupBy)
    val startingNodes = new mutable.Queue() ++ dag.inputNodes
    val visited = new mutable.HashSet[PlanNode]

    startingNodes.foreach((node: PlanNode) => buildDag(node, mapDAG, visited, matches))


    val remainingDAG = buildRemainingDAG(dag, visited)

    visited.clear
    startingNodes.clear
    startingNodes ++= remainingDAG.inputNodes

    val matchesReducer = (n: PlanNode) => n match {
      case v: InputPlanNode => (true, true)
      case v: DistDoPlanNode[_] => (true, true)
      case v: OutputPlanNode => (true, false)
      case v: CombinePlanNode[_, _] => (true, true)
      case _ => (false, false)
    }

    startingNodes.foreach((node: PlanNode) => buildDag(node, reduceDAG, visited, matchesReducer))

    // temporary fields for outputs
    val outputURIs = outputNodes(reduceDAG).map(_.collection.location) ++ (outputNodes(mapDAG).map(_.collection.location).toSet -- reduceDAG.inputNodes.map(_.collection.location))
    val intermediateURIs = outputNodes(mapDAG).map(_.collection.location).toSet.intersect(reduceDAG.inputNodes.map(_.collection.location))

    println(" Reducer = " + reduceDAG)

    val combinedInputs = reduceDAG.filter(_ match {
      case v: CombinePlanNode[_, _] => true
      case _ => false
    }).map(node => (node.inEdges.head._1.asInstanceOf[InputPlanNode].collection.location, node.copyUnconnected().asInstanceOf[CombinePlanNode[Any, Any]]))

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
    intermediateToByte ++= intermediateOutputs.zipWithIndex.map(v => (v._1, v._2.toByte))

    buildRemainingDAG(remainingDAG, visited)
  }

  def configure(job: JobConf) =
    if (!mapDAG.isEmpty) {
      toClean += HadoopJob.dfsSerialize(job, "distribted-collections.mapDAG", mapDAG)
      toClean += HadoopJob.dfsSerialize(job, "distribted-collections.intermediateOutputs", intermediateOutputs)
      toClean += HadoopJob.dfsSerialize(job, "distribted-collections.tempFileToURI", tempFileToURI)
      toClean += HadoopJob.dfsSerialize(job, "distribted-collections.intermediateToByte", intermediateToByte)

      if (!reduceDAG.isEmpty) {
        toClean += HadoopJob.dfsSerialize(job, "distribted-collections.reduceDAG", reduceDAG)
      }
      val addCombiner: Boolean = !combinedInputs.isEmpty

      if (addCombiner) {
        // serialize combined inputs
        toClean += HadoopJob.dfsSerialize(job, "distribted-collections.combinedInputs", combinedInputs)
      }

      // setting input and output and intermediate types
      job.setMapOutputKeyClass(classOf[BytesWritable])
      job.setMapOutputValueClass(classOf[BytesWritable])
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[BytesWritable])
      MultipleOutputs.setCountersEnabled(job, true)

      // set the input and output files for the job
      mapDAG.inputNodes.foreach((in) => FileInputFormat.addInputPath(job, new Path(in.collection.location.toString)))
      FileInputFormat.setInputPathFilter(job, classOf[MetaPathFilter])
      FileOutputFormat.setOutputPath(job, outputDir)


      QuickTypeFixScalaI0.setJobClassesBecause210SnapshotWillNot(job, addCombiner, !reduceDAG.isEmpty, tempFileToURI.keys.toArray)
    }


  def postRun(conf: JobConf, runningJob: RunningJob) = {
    // rename mapper files to part files
    tempFileToURI.foreach(v => {
      // create uri
      val dest = new Path(v._2.toString)
      val fs = FileSystem.get(conf)
      FileSystem.mkdirs(fs, dest, new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ))

      // list all files from temp file
      val stats = fs.listStatus(outputDir);
      stats.foreach(stat => {
        val path = stat.getPath()
        if (path.getName().startsWith(v._1)) {
          FSAdapter.rename(conf, path, new Path(dest, "part-" + path.getName().split("-").last))
        }
      })
      // write collections meta data
      storeCollectionsMetaData(conf, runningJob, v._2, v._1)
    })

    FSAdapter.remove(conf, outputDir.toUri)

    // cleanup all temp files
    toClean.foreach(uri => FSAdapter.remove(conf, uri))

  }

  def storeCollectionsMetaData(conf: JobConf, job: RunningJob, uri: URI, counterName: String) = {
    val size = job.getCounters.getGroup(classOf[MultipleOutputs].getName).getCounter(counterName)

    // write metadata
    val metaDataPath = new Path(new Path(uri.toString), "META")
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(new CollectionMetaData(size))
    oos.flush()
    oos.close()
    FSAdapter.writeToFile(conf, metaDataPath, baos.toByteArray)
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
    val remainingDAG = new ExPlanDAG()
    dag.foreach(node => {
      if (!visited.contains(node)) {
        val nodeCopy = node.copyUnconnected()

        node.inEdges.foreach(edge => if (visited.contains(edge._1)) {
          var existing = remainingDAG.getPlanNode(edge._2)
          if (!existing.isDefined) {
            val newInput = new InputPlanNode(edge._2)
            remainingDAG.addInputNode(newInput)
            existing = Some(newInput)
          }

          existing.get.connect(edge._2, nodeCopy)
        } else {
          // TODO (VJ) fix
          val node = remainingDAG.getPlanNode(edge._1)
          if (node.isDefined) node.get.connect(edge._2, nodeCopy)
        })

        remainingDAG.connectDownwards(nodeCopy, node)
      }
    })
    remainingDAG
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