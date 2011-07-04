import sbt._

class Project(info: ProjectInfo) extends ParentProject(info) with IdeaProject {
  lazy val distributedCollectionsBackendAPI = project("backend-api", "backend-api", new DistributedCollectionsBackendAPIProject(_))
  lazy val hadoopBackend = project("hadoop-backend", "hadoop-backend", new HadoopBackendProject(_))
  lazy val benchmarks = project("benchmarks", "benchmarks", new BenchmarksProject(_))
  lazy val distributedCollections = project("distributed-collections", "distributed-collections", new DistributedCollectionsProject(_))

  /**
   * Common util project dependencies
   */
  trait UtilDependencies {

    // testing frameworks
    val testNG = "org.testng" % "testng" % "5.7" % "test" classifier "jdk15" withSources ()
    val scalaTest = "org.scalatest" % "scalatest" % "1.3" % "test" withSources ()
  }

  class HadoopBackendProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject with UtilDependencies {
    val hadoopCore = "org.apache.mahout.hadoop" % "hadoop-core" % "0.20.1" % "provided"
    val dependsOnDistributedCollectionsBackendAPI = distributedCollectionsBackendAPI
    val kryo = "com.googlecode" % "kryo" % "1.04"

    // needed by hadoop (will be provided on cluster)
    val commonsLogging = "commons-logging" % "commons-logging" % "1.1.1" % "provided"
    val commonsCli = "commons-cli" % "commons-cli" % "1.1" % "provided"
  }
  class DistributedCollectionsProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject with UtilDependencies {
    val dependsOnHadoopBackend = hadoopBackend
    val dependsOnDistributedCollectionsBackendAPI = distributedCollectionsBackendAPI

    override def mainClass = Some("examples.DistCollTest")
  }

  class BenchmarksProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject with UtilDependencies {
    val dependsOnHadoopCore = "org.apache.mahout.hadoop" % "hadoop-core" % "0.20.1" % "provided"
    val dependsOnDistributedCollections = distributedCollections

    // needed by hadoop (will be provided on cluster)
    val commonsLogging = "commons-logging" % "commons-logging" % "1.1.1" % "provided"
    val commonsCli = "commons-cli" % "commons-cli" % "1.1" % "provided"
  }


  class DistributedCollectionsBackendAPIProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject with UtilDependencies {
  }

}
