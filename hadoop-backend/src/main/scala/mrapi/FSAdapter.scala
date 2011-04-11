package mrapi

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{ObjectInputStream, ByteArrayInputStream}
import org.apache.hadoop.io.{SequenceFile, BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.Job

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

object FSAdapter {

  /**
   * Returns an Iterable containing all values from the collection.
   * This operation does not include map and reduce operations but is executed only on master node.
   *
   * TODO Currently it fetches all the elements at once and puts them into a List.
   */
  def valuesTraversable[A](file: URI): Traversable[A] = {
    val filePart = URI.create(file.toString + "/part-r-00000") // to be replaced with proper utility method

    val conf = new Configuration()

    val fs = FileSystem.get(filePart, conf)
    val map = new Path(filePart.toString)

    val reader = new SequenceFile.Reader(fs, map, conf)
    val keyClass = reader.getKeyClass()
    val valueClass = reader.getValueClass()

    val key = new BytesWritable()
    val value = new BytesWritable()

    val result = new scala.collection.mutable.ArrayBuffer[A]()
    while (reader.next(key, value)) {
      val bais = new ByteArrayInputStream(value.getBytes)
      val ois = new ObjectInputStream(bais)
      val valueObject = ois.readObject()

      result += valueObject.asInstanceOf[A]
    }

    reader.close()
    result
  }

  def remove[A](file: URI): Boolean = {
    // TODO this whole class needs to be replace with input output processor
    true
  }

  def writeToFile(job:Job, path: Path, bytes: Array[Byte]) = {
    val file = path.getFileSystem(job.getConfiguration).create(path, true)
    file.write(bytes)
    file.close()
  }

}
