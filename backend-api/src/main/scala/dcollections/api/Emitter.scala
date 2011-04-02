package dcollections.api

/**
 * User: vjovanovic
 * Date: 4/1/11
 */

trait Emitter[A] {
  def emit(elem: A)
}