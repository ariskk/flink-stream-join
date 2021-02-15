package com.ariskk.streamjoin

// Avoid doing a wildcard import in the real world.
// https://medium.com/drivetribe-engineering/towards-achieving-a-10x-compile-time-improvement-in-a-flink-codebase-a69596edcb50
import org.apache.flink.streaming.api.scala._

import FlinkOps._

object FlinkStreamJoin {

  final case class UserId(value: String) extends AnyVal

  final case class User(
    id: UserId,
    firstName: String,
    lastName: String,
    age: Int
  )

  final case class TweetId(value: String) extends AnyVal

  final case class Tweet(
    id: TweetId,
    author: UserId,
    content: String
  )

  final case class TweetView(
    id: TweetId,
    tweet: Tweet,
    author: User
  )

  final case class State(
    user: Option[User],
    tweets: Map[TweetId, Tweet]
  )

  object State {
    lazy val empty: State =
      State(Option.empty[User], Map.empty[TweetId, Tweet])
  }

  /**
    * The following assumes the streams are created using Kafka topics
    * that are properly keyed. Thus, `User` and `Tweet` events will always
    * in the order they were committed to Kafka.
    * For every user event:
    * We fetch all the tweets from state, emit a TweetView per tweet and store it in the state.
    * This deals both with late `User` events and `User` updates.
    * For ever tweet:
    * We fetch the latest user, emit a TweetView and store it in the state.
    */
  def join(
    users: DataStream[User],
    tweets: DataStream[Tweet]
  ): DataStream[TweetView] =
    users
      .connect(tweets)
      .keyBy(_.id, _.author)
      .coMapWithState[Traversable[TweetView], State](
        (user, state) => {
          val views = state.tweets.values.map(t => TweetView(t.id, t, user))
          (views, state.copy(user = Option(user)))
        },
        (tweet, state) => {
          val maybeView = state.user.map(u => TweetView(tweet.id, tweet, u))
          (maybeView, state.copy(tweets = state.tweets + (tweet.id -> tweet)))
        },
        State.empty
      )
      .flatMap(identity(_))

}
