package scala.collection.distributed.api

/**
 * User: vjovanovic
 * Date: 4/1/11
 */


trait Emitter[A] {
  def emit(elem: A)
}

trait Emit[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10] extends IndexedEmit {
  def emit(el: T1): Unit = emit(0, el)

  def emit1(el: T2): Unit = emit(1, el)

  def emit2(el: T3): Unit = emit(2, el)

  def emit3(el: T4): Unit = emit(3, el)

  def emit4(el: T5): Unit = emit(4, el)

  def emit5(el: T6): Unit = emit(5, el)

  def emit6(el: T7): Unit = emit(6, el)

  def emit7(el: T8): Unit = emit(7, el)

  def emit8(el: T9): Unit = emit(8, el)

  def emit9(el: T10): Unit = emit(9, el)
}

trait IndexedEmit {
  def emit(index: Int, el:Any)
}