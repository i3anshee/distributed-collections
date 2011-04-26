package scala.collection.distributed.builder

import scala.collection.distributed.{DistCollection, DistSetOld}

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

class DistSetBuilderFactory[Elem] extends DistCanBuildFrom[DistCollection[Elem], Elem, DistSetOld[Elem]] {

  def apply(from: DistCollection[Elem]) = new DistSetBuilder[Elem]

  def apply() = new DistSetBuilder[Elem]

}