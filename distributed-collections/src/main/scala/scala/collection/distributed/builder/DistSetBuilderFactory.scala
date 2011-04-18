package scala.collection.distributed.builder

import scala.collection.distributed.{DistCollection, DistSet}

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

class DistSetBuilderFactory[Elem] extends DistCanBuildFrom[DistCollection[Elem], Elem, DistSet[Elem]] {

  def apply(from: DistCollection[Elem]) = new DistSetBuilder[Elem]

  def apply() = new DistSetBuilder[Elem]

}