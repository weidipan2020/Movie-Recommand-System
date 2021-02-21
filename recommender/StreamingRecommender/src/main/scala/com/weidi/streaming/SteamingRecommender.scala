package com.weidi.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 定义连接助手对象, 序列化
object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("192.168.1.180")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://192.168.1.180:27017/recommender"))
}


object SteamingRecommender {

  case class MongoConfig(uri: String, db: String)

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

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载电影相似度矩阵数据, 把它广播出去
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("org.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map {
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
    val ratingStream = kafkaStream.map {
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    // 2 继续做流式处理, 核心实时算法部分
    ratingStream.foreachRDD(
      rdds => rdds.foreach {
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

  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongoConfig: MongoConfig): Array[Int] = {
    // 1. 从相似度矩阵中拿到所有相似的电影
    val allSimMovies = simMovies(mid).toArray

    // 2. 从mongodb中查询用户已看过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map {
        item =>
          item.get("mid").toString.toInt
      }

    // 3. 把看过的过滤，得到输出列表
    allSimMovies.filter(
      x =>
        !ratingExist.contains(x._1)
    )
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  def computeMovieScores(candidateMovies: Array[Int],
                         userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int,
                           scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
    // 定义一个ArrayBuffer，用于保存每一个备选电影的基础得分
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    // 定义一个HashMap，保存每一个备选电影的增强减弱因子
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    // 拿到备选电影和最近评分电影的相似度
    for (candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {
      val simScore: Double = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)

      if (simScore > 0.7) {
        // 计算备选电影的基础推荐得分
        scores += ((candidateMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(candidateMovie) = increMap.getOrElse(candidateMovie, 0) + 1
        } else {
          decreMap(candidateMovie) = decreMap.getOrElse(candidateMovie, 0) + 1
        }
      }
    }

    // 根据备选电影的mid做groupby，根据公式去求最后的推荐评分
    // groupBy后是 Map(mid -> Array[(mid, score)])
    scores.groupBy(_._1).map {
      case (mid, scoreList) =>
        (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrElse(mid, 1)) - log(decreMap.getOrElse(mid, 1)))
    }.toArray.sortWith(_._2 > _._2)
  }

  def getMoviesSimScore(mid1: Int, mid2: Int,
                        simMovies: scala.collection.Map[Int,
                          scala.collection.immutable.Map[Int, Double]]): Double = {
    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  def log(n: Int): Double = {
    val m = 10 // 以10为底
    math.log(n) / math.log(m)
  }

  def saveDateToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig) = {
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    // 如果表中已有uid对应的数据，则删除
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))

    // 将streamRecs数据存入表中
    streamRecsCollection.insert(MongoDBObject("uid" -> uid,
      "recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))
  }
}
