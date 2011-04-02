package tasks

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}

/**
 * User: vjovanovic
 * Date: 4/2/11
 */

trait CollectionTask {
  // TODO (VJ) add different serialization policies
  def serializeElement(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.flush()
    baos.toByteArray
  }

  def deserializeElement(bytes: Array[Byte]): AnyRef = {
    val bais = new ByteArrayInputStream(v.getBytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject()
  }
}