import sbt._
class Project(info: ProjectInfo) extends ParentProject(info) with IdeaProject {
  lazy val hadoopBackend   = project("hadoop-backend", "hadoop-backend", new HadoopBackendProject(_))
  lazy val distributedCollections  = project("distributed-collections", "distributed-collections", new DistributedCollectionsProject(_))

  /**
   * Common project dependencies
   */
  trait CommonProjectDependencies {

    // testing frameworks
    val testNG = "org.testng" % "testng" % "5.7" % "test" classifier "jdk15" withSources ()
    val scalaTest = "org.scalatest" % "scalatest" % "1.3" % "test" withSources ()
  }

  class HadoopBackendProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject with CommonProjectDependencies {

  }

  class DistributedCollectionsProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject with CommonProjectDependencies {
    val dependsOnHadoopBackend = hadoopBackend
  }

}
