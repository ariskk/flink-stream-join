package com.ariskk.streamjoin

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source._

final class SlowSource[T: TypeInformation](
  elements: Iterable[T],
  initDelay: Long,
  emitEvery: Long
) extends RichSourceFunction[T] {

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    // Slow to start
    Thread.sleep(initDelay)
    // Slow to emit
    elements
      .map { i =>
        Thread.sleep(emitEvery)
        i
      }
      .foreach(ctx.collect)
  }

  override def cancel(): Unit = {}
}
