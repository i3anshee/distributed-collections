package dcollections.builder

import dcollections.DistCollection

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

class DistCollectionBuilderFactory[Elem] extends DistCanBuildFrom[DistCollection[Elem], Elem, DistCollection[Elem]] {
  def apply(from: DistCollection[Elem]) = new DistCollectionBuilder[Elem]

  def apply() = new DistCollectionBuilder[Elem]
}