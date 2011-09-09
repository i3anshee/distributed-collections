package de.jungblut.clustering.collections

import io.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import de.jungblut.clustering.model.ClusterCenter
import scala.Tuple2
/**
 * @author Vojin Jovanovic
 */

class KryoRegistratorKMeans extends KryoRegistrator {
  def registerClasses(kryo: Kryo) = {
    kryo.register(classOf[de.jungblut.clustering.model.Vector])
    kryo.register(classOf[ClusterCenter])
    kryo.register(classOf[scala.Tuple2[ClusterCenter, de.jungblut.clustering.model.Vector]])
  }
}