package com.weidi

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object OfflineRecommender {

  // 基于评分数据的LFM, 只需要rating数据
  case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

  case class MongoConfig(uri: String, db: String)

  // 定义一个基准推荐对象
  case class Recommendation(mid: Int, score: Double)

  // 定义基于预测评分的用户推荐列表
  case class UserRecs(uid: Int, recs: Seq[Recommendation])

  // 定义基于LFM电影特征向量的电影相似度列表
  case class MovieRecs(mid: Int, recs: Seq[Recommendation])

  // 定义表名和常量
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.1.180:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score))
      .cache()

    // 从rating数据中提取所有的uid和mid, 并去重

    // 训练隐语义模型

    // 基于用户和电影的隐特征, 计算预测评分, 得到用户的推荐列表
    // 计算user和movie的笛卡尔积, 得到一个空评分矩阵

    // 调用model的predict方法预测评分

    // 基于电影隐特征, 计算相似度矩阵, 得到电影的相似度列表

    // 对所有电影两两计算它们的相似度, 先做笛卡尔积


  }

  // 求向量余弦相似度
}
