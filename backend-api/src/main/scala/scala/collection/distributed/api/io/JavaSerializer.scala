/**
 * Copyright (c) 2010, Regents of the University of California.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *  * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of California, Berkeley nor the
 *      names of its contributors may be used to endorse or promote
 *      products derived from this software without specific prior written
 *      permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Taken from Spark project (www.spark-project.org) for quick benchmarking. Thanks Matei.
 */
package scala.collection.distributed.api.io


import java.io._

class JavaSerializationStream(out: OutputStream) extends SerializationStream {
  val objOut = new ObjectOutputStream(out)
  def writeObject[T](t: T) { objOut.writeObject(t) }
  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

class JavaDeserializationStream(in: InputStream) extends DeserializationStream {
  val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) =
      Class.forName(desc.getName, false, currentThread.getContextClassLoader)
  }

  def readObject[T](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}

class JavaSerializerInstance extends SerializerInstance {
  def serialize[T](t: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = outputStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val in = inputStream(bis)
    in.readObject().asInstanceOf[T]
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s)
  }
}

class JavaSerializer extends Serializer {
  def newInstance(): SerializerInstance = new JavaSerializerInstance
}