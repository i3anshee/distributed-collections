/*
 * Code for k-means clustering is taken from Thomas Jungblut at http://code.google.com/p/hama-shortest-paths.
 * Thanks Thomas.
 */

package de.jungblut.clustering.model;

public class DistanceMeasurer {

	/**
	 * Manhattan stuffz.
     * @param center cluster center
     * @param v vector
     * @return manhattan distance of vector v from center
     */
	public static double measureDistance(ClusterCenter center, Vector v) {
		double sum = 0;
		int length = v.getVector().length;
		for (int i = 0; i < length; i++) {
			sum += Math.abs(center.getCenter().getVector()[i]
					- v.getVector()[i]);
		}

		return sum;
	}

}
