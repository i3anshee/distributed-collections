package scala.colleciton.distributed.hadoop

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, SequenceFile, BytesWritable}
import java.io.{ObjectOutputStream, ByteArrayOutputStream, ObjectInputStream, ByteArrayInputStream}
import scala.collection.distributed.api.io.CollectionMetaData
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.mapred.JobConf

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

object FSAdapter {

  /**
   * Returns an Iterable containing all values from the collection.
   * This operation does not include map and reduce operations but is executed only on master node.
   *
   */
  def valuesIterable[A](file: URI): scala.collection.immutable.Iterable[A] = {
    val filePart = URI.create(file.toString + "/part-r-00000") // to be replaced with proper utility method

    val conf = new Configuration()

    val fs = FileSystem.get(filePart, conf)
    val map = new Path(filePart.toString)

    val reader = new SequenceFile.Reader(fs, map, conf)
    val keyClass = reader.getKeyClass()
    val valueClass = reader.getValueClass()
    val key = NullWritable.get
    val value = new BytesWritable()

    val result = new scala.collection.mutable.ArrayBuffer[A]()
    while (reader.next(key, value)) {
      val bais = new ByteArrayInputStream(value.getBytes)
      val ois = new ObjectInputStream(bais)
      val valueObject = ois.readObject()

      result += valueObject.asInstanceOf[A]
    }

    reader.close()
    result.toList
  }

  def remove[A](file: URI): Boolean = {
    // TODO this whole class needs to be replace with input output processor
    true
  }

  def writeToFile(job: JobConf, path: Path, bytes: Array[Byte]) = {
    val file = path.getFileSystem(job).create(path, true)
    file.write(bytes)
    file.close()
  }

  def createDistCollection(t: TraversableOnce[_], uri: URI) = {
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
        .write(serializeElement(new CollectionMetaData(t.size)));

      writer = Some(new SequenceFile.Writer(fs, conf, file, classOf[NullWritable], classOf[BytesWritable]))
      t.foreach(v => writer.get.append(NullWritable.get, new BytesWritable(serializeElement(v))))
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

//  def listFiles(job: JobConf, dir: Path, regex:String): List[String] {
//    vaconf = new Configuration() // takes default conf
//        FileSystem fs = FileSystem.get(conf);
//        Path dir = new Path("/dir");
//        FileStatus[] stats = fs.listStatus(dir);
//        foreach(FileStatus stat : stats)
//        {
//            stat.getPath().toUri().getPath(); // gives directory name
//            stat.getModificationTime();
//            stat.getReplication();
//            stat.getBlockSize();
//            stat.getOwner();
//            stat.getGroup();
//            stat.getPermission().toString();
//        }

//  }

  def rename(conf: JobConf, from: Path, to: Path) {
    val fs = FileSystem.get(conf)
    fs.rename(from, to)
    fs.close
  }


  private def serializeElement(value: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.flush()
    baos.toByteArray
  }

}