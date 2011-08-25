package scala.colleciton.distributed.hadoop

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, SequenceFile, BytesWritable}
import java.io.{ObjectOutputStream, ByteArrayOutputStream, ObjectInputStream, ByteArrayInputStream}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.{FileSystem, Path}
import collection.immutable
import io.KryoSerializer
import collection.distributed.api.io.{JavaSerializerInstance, SerializerInstance, CollectionMetaData}

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

object FSAdapter {

  val kryo = new KryoSerializer().newInstance()
  val javaSerializer = new JavaSerializerInstance

  // TODO (VJ) add  read buffers, synchronization and move to the api
  /**
   *
   * Returns an Iterable containing all values from the collection.
   * This operation does not include map and reduce operations but is executed only on master node.
   *
   */
  def valuesIterable[A](file: URI): immutable.Iterable[A] = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)


    val result = new scala.collection.mutable.ArrayBuffer[A]()

    // go through files in order and collect elements
    val files: Seq[Path] = fs.listStatus(new Path(file.toString), new CollectionsMetaDataPathFilter).toSeq.map(_.getPath())
    files.sortWith((v1, v2) => v1.getName.split("-").last < v1.getName.split("-").last).foreach(filePart => {
      val map = new Path(filePart.toString)

      val reader = new SequenceFile.Reader(fs, map, conf)
      val keyClass = reader.getKeyClass()
      val valueClass = reader.getValueClass()
      val key = NullWritable.get
      val value = new BytesWritable()

      while (reader.next(key, value)) {
        result += kryo.deserialize(value.getBytes)
      }
      reader.close()
    })


    result.toList
  }

  def remove[A](conf: JobConf, file: URI): Boolean = {
    FileSystem.get(file, conf).delete(new Path(file.toString), true)
  }

  def writeToFile(job: JobConf, path: Path, bytes: Array[Byte]) = {
    val file = path.getFileSystem(job).create(path, true)
    file.write(bytes)
    file.close()
  }

  def createDistCollection(t: TraversableOnce[_], uri: URI, serializer: SerializerInstance) = {
    val conf = new Configuration()

    val fs = FileSystem.get(uri, conf)
    val dir = new Path(uri.toString)
    val file = new Path(uri.toString + "/part-r-00000")
    val meta = new Path(uri.toString + "/META")
    FileSystem.mkdirs(fs, dir, new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ))

    // write elements to file
    var writer: Option[SequenceFile.Writer] = None
    try {
      // write length to metadata
      FileSystem.create(fs, meta, new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ))
        .write(javaSerializer.serialize(new CollectionMetaData(t.size)));

      writer = Some(new SequenceFile.Writer(fs, conf, file, classOf[NullWritable], classOf[BytesWritable]))
      t.foreach(v => writer.get.append(NullWritable.get, new BytesWritable(serializer.serialize(v))))
    } finally {
      if (writer.isDefined) {
        writer.get.close()
      }
    }

    // make file read only
    fs.setPermission(dir, new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ))
    fs.setPermission(file, new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ))
    fs.setPermission(meta, new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ))
  }

  def rename(conf: JobConf, from: Path, to: Path) {
    val fs = FileSystem.get(conf)
    fs.rename(from, to)
  }

}
