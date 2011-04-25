package scala.collection.distributed

import java.net.URI

/**
 * User: vjovanovic
 * Date: 3/29/11
 */

class DistMap[K, V](location:URI) extends DistColl[(K, V)](location) {
}
