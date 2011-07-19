package de.jungblut.clustering.collections

import de.jungblut.clustering.model.{Vector, DistanceMeasurer, ClusterCenter}
import collection.mutable.{ArrayBuffer, Buffer}
import java.net.URI
import collection.distributed.shared.{DistCounter, DistIterableBuilder}
import execution.ExecutionPlan
import collection.immutable.GenIterable
import collection.distributed.api.{DistContext, Emitter2}
import collection.distributed.{DistIterable, DistCollection}

/**
 * @author Vojin Jovanovic
 */

object KMeansClustering {
  def main(args: Array[String]) {
    System.setProperty("spark.kryo.registrator", "de.jungblut.clustering.collections.KryoRegistratorKMeans")

    var points: DistIterable[(ClusterCenter, Vector)] =
      new DistCollection[(ClusterCenter, Vector)](new URI("tmp/clustering/kryo-input"))
    var centers: DistIterable[ClusterCenter] = new DistCollection[ClusterCenter](new URI("tmp/clustering/kryo-cen"))
    val convergedCounter = DistCounter()

    var converged = false
    while (!converged) {
      val seqCenters = centers.seq
      val distCenters = DistIterableBuilder[ClusterCenter]()
      val result = points.map(v => {
        var nearest: ClusterCenter = null
        var nearestDistance: Double = Double.MaxValue

        for (c <- seqCenters) {
          val dist = DistanceMeasurer.measureDistance(c, v._2)
          if (nearest == null) {
            nearest = c
            nearestDistance = dist
          } else {
            if (nearestDistance > dist) {
              nearest = c
              nearestDistance = dist
            }
          }
        }

        (nearest, v._2)
      }).groupByKey.flatMap((values: (ClusterCenter, GenIterable[Vector])) => {
        var newCenter: Vector = new Vector
        val vectorList: Buffer[Vector] = new ArrayBuffer[Vector]
        val vectorSize: Int = values._1.getCenter.getVector.length

        newCenter.setVector(new Array[Double](vectorSize))

        values._2.foreach((v: Vector) => {
          {
            vectorList += v

            //sum all the vectors from cluster
            var i: Int = 0
            while (i < v.getVector.length) {
              newCenter.getVector()(i) += v.getVector()(i)
              i += 1;
            }
          }
        })

        newCenter = new Vector(newCenter.getVector.map(v => v / values._2.size))
        val center: ClusterCenter = new ClusterCenter(newCenter)

        // closure side effects
        distCenters += center
        if (center.converged(values._1)) convergedCounter += 1

        vectorList.map(v => (center, v))
      })


      points = result
      centers = distCenters.result

      converged = (convergedCounter() == seqCenters.size)
      convergedCounter += -convergedCounter()
    }
  }

}