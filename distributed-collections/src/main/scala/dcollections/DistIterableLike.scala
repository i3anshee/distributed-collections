package dcollections

import builder.DistCanBuildFrom

/**
 * User: vjovanovic
 * Date: 4/12/11
 */

trait DistIterableLike[+A, +Repr] {

//  def parallelDo()
//
//  def repr: Repr = this.asInstanceOf[Repr]
//
//  /**Builds a new collection by applying a function to all elements of this $coll.
//   */
//  def map[B, That](f: A => B)(implicit bf: DistCanBuildFrom[Repr, B, That]): That = {
//    val builder = bf(repr)
//    builder += parallelDo((el, emitter) => emitter.emit(f(el)))
//    builder.result
//  }


}