package scala.collection.distributed.api


trait IndexedEmitter {
  def emit(index: Int, el: Any)
}

trait UntypedEmitter extends Emitter10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]

trait Emitter[T] extends IndexedEmitter {
  def emit(el: T): Unit = emit(0, el)

  def emit1(el: T): Unit = emit(0, el)
}

trait Emitter2[T1, T2] extends Emitter[T1] {
  def emit2(el: T2): Unit = emit(1, el)
}

trait Emitter3[T1, T2, T3] extends Emitter2[T1, T2] {
  def emit3(el: T2): Unit = emit(2, el)
}

trait Emitter4[T1, T2, T3, T4] extends Emitter3[T1, T2, T3] {
  def emit4(el: T2): Unit = emit(3, el)
}

trait Emitter5[T1, T2, T3, T4, T5] extends Emitter4[T1, T2, T3, T4] {
  def emit5(el: T2): Unit = emit(4, el)
}

trait Emitter6[T1, T2, T3, T4, T5, T6] extends Emitter5[T1, T2, T3, T4, T5] {
  def emit6(el: T2): Unit = emit(5, el)
}

trait Emitter7[T1, T2, T3, T4, T5, T6, T7] extends Emitter6[T1, T2, T3, T4, T5, T6] {
  def emit7(el: T2): Unit = emit(6, el)
}

trait Emitter8[T1, T2, T3, T4, T5, T6, T7, T8] extends Emitter7[T1, T2, T3, T4, T5, T6, T7] {
  def emit8(el: T8): Unit = emit(7, el)
}

trait Emitter9[T1, T2, T3, T4, T5, T6, T7, T8, T9] extends Emitter8[T1, T2, T3, T4, T5, T6, T7, T8] {
  def emit9(el: T9): Unit = emit(8, el)
}

trait Emitter10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10] extends Emitter9[T1, T2, T3, T4, T5, T6, T7, T8, T9] {
  def emit10(el: T10): Unit = emit(9, el)
}
