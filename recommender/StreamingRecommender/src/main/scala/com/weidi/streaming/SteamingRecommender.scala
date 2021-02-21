package com.weidi.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object SteamingRecommender {
  case class MongoCofig(uri: String, db: String)

  case class Recommendation(mid: Int, score: Double)

  case class MovieRecs(mid: Int, recs: Seq[Recommendation])

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.1.180:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val sparkConfig = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._

    implicitly val mongoConfig = MongoCofig(config("mongo.uri"), config("mongo.db"))

    // 加载电影相似度矩阵数据, 把它广播出去
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("org.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{
        movieRecs =>
          (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap)
      }
      .collectAsMap()

    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)

    // 定义kafka链接参数
    val kafkaParam = Map(
      "bootstrap.servers" -> "192.168.1.180:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    // 通过kafka创建一个DStream
    val kafkaStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
    )

    // 把原始数据UID|MID|SCORE|TIMESTAMP 转换成评分流
    val ratingStream = kafkaStream.map{
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    // 2 继续做流式处理, 核心实时算法部分
    ratingStream.foreachRDD(
      rdds => rdds.foreach{
        case (uid, mid, score, timestamp) => {
          println("rating data coming! >>>>>>>>>>>>>>")
          // 2.1 从redis里获得当前用户最近的k次评分, 保存成Array[(mid, score)]
          val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, mid, ConnHelper.jedis)

          // 2.2 从相似度矩阵中取出当前电影最相似的N个电影, 作为备选列表, Array[mid]
          val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

          // 2.3 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid, score)]
          val streamRecs = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)

          // 2.4 把推荐数据保存到mongodb
          saveDateToMongoDB(uid, streamRecs)

        }
      }
    )

    println("streaming started!")

  }

  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从redis读取数据，用户评分数据保存在 uid:UID 为key的队列里，value是 MID:SCORE
    import scala.collection.JavaConversions._
    jedis.lrange("uid:" + uid, 0, num - 1)
      .map {
        item =>
          val attr = item.split("\\:")
          (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongoCofig: MongoCofig) : Array[Int] = {
    // 1. 从相似度矩阵中拿到所有相似的电影

    // 2. 从mongodb中查询用户已看过的电影

    // 3. 把看过的过滤，得到输出列表
  }
}
