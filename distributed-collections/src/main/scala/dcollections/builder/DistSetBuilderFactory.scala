package dcollections.builder

import dcollections.{DistCollection, DistSet}

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

class DistSetBuilderFactory[Elem] extends DistCanBuildFrom[DistCollection[Elem], Elem, DistSet[Elem]] {

  def apply(from: DistCollection[Elem]) = new DistSetBuilder[Elem]

  def apply() = new DistSetBuilder[Elem]

}