package tasks

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import dcollections.api.{RecordNumber, Emitter}

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

trait CollectionTask {

  // TODO (VJ) add different serialization policies
  def serializeElement(value: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.flush()
    baos.toByteArray
  }

  def deserializeElement(bytes: Array[Byte]): AnyRef = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject()
  }

  def deserializeOperation[T](conf: Configuration, name: String): Option[T] = {
    // find the file in the node local cache
    val opFileURI = conf.get(name)
    if (opFileURI == null)
      None
    else {
      val cacheFiles = DistributedCache.getCacheFiles(conf)
      val opFile = new Path(cacheFiles.filter((x: URI) => x.toString == opFileURI)(0).toString)
      val inputStream = new java.io.ObjectInputStream(opFile.getFileSystem(conf).open(opFile))
      val result = Some(inputStream.readObject().asInstanceOf[T])
      inputStream.close()
      result
    }
  }

  def parallelDo(elems: Traversable[AnyRef], emitter: EmiterImpl, recordNumber:RecordNumber,
                 parTask: Option[(AnyRef, Emitter[AnyRef], RecordNumber) => Unit]): TraversableOnce[AnyRef] = {
    if (parTask.isDefined) {
      elems.foreach((el) => {
        parTask.get(el, emitter, recordNumber)
      })
      val newElems = emitter.getBuffer
      emitter.clear
      newElems
    } else
      elems
  }
}