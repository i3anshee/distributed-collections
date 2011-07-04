package de.jungblut.clustering.collections

import de.jungblut.clustering.model.{Vector, DistanceMeasurer, ClusterCenter}
import collection.mutable.{ArrayBuffer, Buffer}
import collection.distributed.{DistCollection}
import java.net.URI
import collection.distributed.shared.DSECounter

/**
 * @author Vojin Jovanovic
 */

class KMeansClustering {
  def main(args: Array[String]) {
    val points = new DistCollection[(ClusterCenter, Vector)](new URI("tmp/clustering/input"))
    val centers = List(new ClusterCenter(new Vector(1, 1)), new ClusterCenter(new Vector(5, 5)))

    val convergedCounter = DSECounter()

    points.map(v => {
      var nearest: ClusterCenter = null
      var nearestDistance: Double = Double.MaxValue

      for (c <- centers) {
        val dist = DistanceMeasurer.measureDistance(c, v._2)
        if (nearest == null) {
          nearest = c
          nearestDistance = dist
        }
        else {
          if (nearestDistance > dist) {
            nearest = c
            nearestDistance = dist
          }
        }
      }

      (nearest, v._2)
    }).groupByKey.flatMap(values => {
      var newCenter: Vector = new Vector
      val vectorList: Buffer[Vector] = new ArrayBuffer[Vector]
      val vectorSize: Int = values._1.getCenter.getVector.length

      newCenter.setVector(new Array[Double](vectorSize))
      values._2.map((v: Vector) => {
        {
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
      //      centers.add(center)
      //      for (vector <- vectorList) {
      //        context.write(center, vector)
      //      }
      //
      if (center.converged(values._1)) convergedCounter += 1
      throw new UnsupportedOperationException()
    })
  }

}