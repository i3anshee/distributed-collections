/**
 * Taken from Spark project (www.spark-project.org) for quick benchmarking. Thanks Matei.
 */
package io

/**
 * @author Vojin Jovanovic
 */

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.immutable
import scala.collection.mutable

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import collection.distributed.api.io.{SerializationStream, DeserializationStream, SerializerInstance}
import java.lang.Class

/**
 * Zig-zag encoder used to write object sizes to serialization streams.
 * Based on Kryo's integer encoder.
 */
object ZigZag {
  def writeInt(n: Int, out: OutputStream) {
    var value = n
    if ((value & ~0x7F) == 0) {
      out.write(value)
      return
    }
    out.write(((value & 0x7F) | 0x80))
    value >>>= 7
    if ((value & ~0x7F) == 0) {
      out.write(value)
      return
    }
    out.write(((value & 0x7F) | 0x80))
    value >>>= 7
    if ((value & ~0x7F) == 0) {
      out.write(value)
      return
    }
    out.write(((value & 0x7F) | 0x80))
    value >>>= 7
    if ((value & ~0x7F) == 0) {
      out.write(value)
      return
    }
    out.write(((value & 0x7F) | 0x80))
    value >>>= 7
    out.write(value)
  }

  def readInt(in: InputStream): Int = {
    var offset = 0
    var result = 0
    while (offset < 32) {
      val b = in.read()
      if (b == -1) {
        throw new EOFException("End of stream")
      }
      result |= ((b & 0x7F) << offset)
      if ((b & 0x80) == 0) {
        return result
      }
      offset += 7
    }
    throw new Exception("Malformed zigzag-encoded integer")
  }
}

class KryoSerializationStream(kryo: Kryo, buf: ByteBuffer, out: OutputStream)
  extends SerializationStream {
  def writeObject[T](t: T) {
    kryo.writeClassAndObject(buf, t)
    ZigZag.writeInt(buf.position(), out)
    buf.flip()
    Channels.newChannel(out).write(buf)
    buf.clear()
  }

  def flush() {
    out.flush()
  }

  def close() {
    out.close()
  }
}

class KryoDeserializationStream(buf: ObjectBuffer, in: InputStream)
  extends DeserializationStream {
  def readObject[T](): T = {
    val len = ZigZag.readInt(in)
    buf.readClassAndObject(in, len).asInstanceOf[T]
  }

  def close() {
    in.close()
  }
}

class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {
  val buf = ks.threadBuf.get()

  def serialize[T](t: T): Array[Byte] = {
    buf.writeClassAndObject(t)
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    buf.readClassAndObject(bytes).asInstanceOf[T]
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(ks.kryo, ks.threadByteBuf.get(), s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(buf, s)
  }
}

// Used by clients to register their own classes
trait KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit
}

class KryoSerializer extends Serializer {
  val kryo = createKryo()


  // TODO (VJ) what are these two methods?
  def writeObjectData(p1: ByteBuffer, p2: AnyRef) = {}

  def readObjectData[T](p1: ByteBuffer, p2: Class[T]) = throw new UnsupportedOperationException()

  val bufferSize =
    1 * 1024 * 1024

  val threadBuf = new ThreadLocal[ObjectBuffer] {
    override def initialValue = new ObjectBuffer(kryo, bufferSize)
  }

  val threadByteBuf = new ThreadLocal[ByteBuffer] {
    override def initialValue = ByteBuffer.allocate(bufferSize)
  }

  def createKryo(): Kryo = {
    val kryo = new Kryo()

    // Register some commonly used classes
    val toRegister: Seq[AnyRef] = Seq(
      // Arrays
      Array(1), Array(1.0), Array(1.0f), Array(1L), Array(""), Array(("", "")),
      Array(new java.lang.Object),
      // Specialized Tuple2s
      ("", ""), (1, 1), (1.0, 1.0), (1L, 1L),
      (1, 1.0), (1.0, 1), (1L, 1.0), (1.0, 1L), (1, 1L), (1L, 1),
      // Scala collections
      List(1), immutable.Map(1 -> 1), immutable.HashMap(1 -> 1), (0 to 1),
      mutable.Map(1 -> 1), mutable.HashMap(1 -> 1), mutable.ArrayBuffer(1),
      // Options and Either
      Some(1), Left(1), Right(1),
      // Higher-dimensional tuples
      (1, 1, 1), (1, 1, 1, 1), (1, 1, 1, 1, 1)
    )
    for (obj <- toRegister) {
      kryo.register(obj.getClass)
    }

    // Register some commonly used Scala singleton objects. Because these
    // are singletons, we must return the exact same local object when we
    // deserialize rather than returning a clone as FieldSerializer would.
    kryo.register(None.getClass, new KSerializer {
      override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {}

      override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = None.asInstanceOf[T]
    })
    kryo.register(Nil.getClass, new KSerializer {
      override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {}

      override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = Nil.asInstanceOf[T]
    })

    kryo.register(::.getClass, new KSerializer {
      override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {}

      override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = ::.asInstanceOf[T]
    })

    val regCls = System.getProperty("spark.kryo.registrator")
    println(regCls)
    if (regCls != null) {
      val reg = Class.forName(regCls).newInstance().asInstanceOf[KryoRegistrator]
      reg.registerClasses(kryo)
    }
    kryo
  }

  def newInstance(): SerializerInstance = new KryoSerializerInstance(this)
}