# flink-stream-join

Tiny demo project that demonstrates how to join streams of Kafka events using Apache Flink.

This is a solution to a question I have been using in interviews to test for distributed stream processing knowledge.

The question goes as follows:

> Assume you have the following rudimentary data model. 
> Assume that `User` and `Tweet` are keyed using their respective keys and stored in Kafka.
> Describe how you would implement a `join` function to produce `DataStream[TweetView]` from a `DataStream[User]` and a `DataStream[Tweet]`.
> Choose a streaming framework you are comfortable with. Kafka's `KStream` or Spark's `DStream` could work equally well.

```scala
case class User(userId: UserId, firstName: String, age: Int)
case class Tweet(tweetId: TweetId, author: UserId, content: String)
case class TweetView(tweetId: TweetId, tweet: Tweet, author: User)

val users: DataStream[User] = fromKafkaTopic("users")
val tweets: DataStream[Tweet] = fromKafkaTopic("tweets")

def join(users: DataStream[User], tweets: DataStream[Tweet]): DataStream[TweetView] = {
  ???
}
```

Any solution is fair game, assuming it takes into account `users` and `tweets` can arrive in any order.

