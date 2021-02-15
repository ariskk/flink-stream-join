package com.ariskk.streamjoin

import scala.util.Random

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flink.streaming.api.scala._

import FlinkStreamJoin._

class FlinkStreamJoinSpec extends AnyFunSpec with Matchers {

  describe("FlinkStreamJoin") {
    it("shouldn't emit any views if the `User` is missing") {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val userId = UserId("user123")
      val tweets = (1 to 5).map(i => Tweet(TweetId(s"tweet$i"), userId, s"content$i"))

      val tweetStream = env.fromCollection(tweets)

      val out = join(env.fromCollection[User](Seq.empty), tweetStream).executeAndCollect()

      out.toList.size should equal(0)

      out.close()

    }

    it("should emit a `Tweet` with the most recent version of the `User` last") {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val userId = UserId("user123")
      val users = (1 to 5).map(i => User(userId, "Mike", s"$i", 20))
      val tweet = Tweet(TweetId("tweet1"), userId, "amazing tweet")

      val out = join(env.fromCollection(users), env.fromElements(tweet)).executeAndCollect()

      out.toList.last.author.lastName should equal("5")

      out.close()

    }

    it("should cater for late events") {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val users = (0 to 2).map(i => User(UserId(s"user$i"), "User", s"$i", 20))
      val tweets = (0 to 5).map(i => Tweet(TweetId(s"Tweet$i"), users(i % 3).id, s"content $i"))

      val views = tweets.zipWithIndex.map { case (t, idx) => TweetView(t.id, t, users(idx % 3)) }

      // Dreadfully slow source
      // The first user will arrive 100 millis after the start of the program
      val slowUserSource = new SlowSource[User](users, 100, 50)

      val out = join(env.addSource(slowUserSource), env.fromCollection(tweets)).executeAndCollect()

      val list = out.toList

      list should contain theSameElementsAs views

      out.close()

    }

    it("should deal with a real world mess") {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val users = (0 to 2).map(i => User(UserId(s"user$i"), "User", s"$i", 20))
      val updates = Random.shuffle(users.map(_.copy(firstName = "User v2")))

      val allUserEvents = new SlowSource[User](users ++ updates, 100, 20)

      val tweets = (0 to 5).map(i => Tweet(TweetId(s"Tweet$i"), users(i % 3).id, s"content $i"))
      val tweetUpdates = Random.shuffle(tweets.map(x => x.copy(content = s"${x.content} v2")))

      val allTweetEvents = new SlowSource[Tweet](tweets ++ tweetUpdates, 10, 40)

      val views = (0 to 5).map { i =>
        val id = TweetId(s"Tweet$i")
        TweetView(
          id,
          Tweet(id, users(i % 3).id, s"content $i v2"),
          User(UserId(s"user${i % 3}"), "User v2", s"${i % 3}", 20)
        )
      }

      val out = join(
        env.addSource(allUserEvents),
        env.addSource(allTweetEvents)
      ).executeAndCollect()

      val list = out.toList.groupBy(_.id).mapValues(_.last).values.toList

      list should contain theSameElementsAs views

      out.close()

    }

  }

}
