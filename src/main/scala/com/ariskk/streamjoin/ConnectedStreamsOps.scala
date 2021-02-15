package com.ariskk.streamjoin

import scala.reflect.ClassTag

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream}

trait ConnectedStreamsOps[A, B] {

  def cs: ConnectedStreams[A, B]

  def coMapWithState[O: ClassTag: TypeInformation, S: ClassTag: TypeInformation](
    f1: (A, S) => (O, S),
    f2: (B, S) => (O, S),
    emptyState: S
  ): DataStream[O] =
    cs.map(
      new RichCoMapFunction[A, B, O] {
        lazy val stateTypeInfo: TypeInformation[S] = implicitly[TypeInformation[S]]
        lazy val serializer: TypeSerializer[S] = stateTypeInfo.createSerializer(getRuntimeContext.getExecutionConfig)
        lazy val stateDescriptor = new ValueStateDescriptor[S]("name", serializer)

        def map[I](in: I, f: (I, S) => (O, S)): O = {
          val state = getRuntimeContext.getState(stateDescriptor)
          val (o, newState) = f(in, Option(state.value).getOrElse(emptyState))
          state.update(newState)
          o
        }

        override def map1(in: A): O = map[A](in, f1)

        override def map2(in: B): O = map[B](in, f2)
      }
    )

}

object FlinkOps {
  implicit class ConnectedStreamOps[A, B](val cs: ConnectedStreams[A, B]) extends ConnectedStreamsOps[A, B]
}
