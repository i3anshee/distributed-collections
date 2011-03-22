package util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import org.apache.hadoop.io._
import java.io.{ByteArrayInputStream, ObjectInputStream}

/**
 * User: vjovanovic
 * Date: 3/22/11
 */

object ListSeqFile {

  def main(args: Array[String]) {
    val conf = new Configuration()

    val fs = FileSystem.get(URI.create(args(0)), conf)
    val map = new Path(args(0))

    val reader = new SequenceFile.Reader(fs, map, conf)
    val keyClass = reader.getKeyClass()
    val valueClass = reader.getValueClass()

    println(keyClass)
    println(valueClass)

    val key = new LongWritable();
    val value = new BytesWritable();

    while (reader.next(key, value)) {
      val bais = new ByteArrayInputStream(value.getBytes);
      val ois = new ObjectInputStream(bais);
      val valueObject = ois.readObject()

      println("[" + key.toString + "," + valueObject.toString + "]")
    }

    reader.close()
  }

}