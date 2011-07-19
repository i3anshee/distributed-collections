package scala.collection.distributed

trait RichDistProcessable[+T] extends DistProcessable[T] {

  def flatten[B >: T](distIterable1: DistIterable[B]): DistIterable[T] = flatten(List(distIterable1))

  def flatten[B >: T, C >: T](distIterable1: DistIterable[B], distIterable2: DistIterable[C]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2))

  def flatten[B >: T, C >: T, D >: T](distIterable1: DistIterable[B], distIterable2: DistIterable[C],
                                      distIterable3: DistIterable[D]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2, distIterable3))

  def flatten[B >: T, C >: T, D >: T, E >: T](distIterable1: DistIterable[B], distIterable2: DistIterable[C],
                                              distIterable3: DistIterable[D], distIterable4: DistIterable[E]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2, distIterable3, distIterable4))

  def flatten[B >: T, C >: T, D >: T, E >: T, F >: T]
  (distIterable1: DistIterable[B], distIterable2: DistIterable[C],
   distIterable3: DistIterable[D], distIterable4: DistIterable[E],
   distIterable5: DistIterable[F]): DistIterable[T] =
    flatten(List(distIterable1, distIterable2, distIterable3, distIterable4, distIterable5))
}