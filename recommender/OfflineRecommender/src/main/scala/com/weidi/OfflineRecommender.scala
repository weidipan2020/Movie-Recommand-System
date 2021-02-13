package com.weidi

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.jblas.DoubleMatrix

/**
 * 总结: 1. 知道jblas库下的DoubleMatrix类可以帮助做矩阵代数运算
 *      2. 知道groupByKey后iter的对象可toList后再进行处理
 */

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
    val uidRDD = ratingRDD.map(_._1).distinct()
    val midRDD = ratingRDD.map(_._2).distinct()

    // 训练隐语义模型
    val ALSRatingRDD = ratingRDD.map(rating => Rating(rating._1, rating._2, rating._3))
    val als = new ALS()
    val model = als.run(ALSRatingRDD)


    // 基于用户和电影的隐特征, 计算预测评分, 得到用户的推荐列表
    // 计算user和movie的笛卡尔积, 得到一个空评分矩阵
    val matrixUidMid = uidRDD.cartesian(midRDD)

    // 调用model的predict方法预测评分
    model.predict(matrixUidMid)

    // 基于电影隐特征, 计算相似度矩阵, 得到电影的相似度列表
    val mFeatures = model.productFeatures.map{
      case (mid, features) => (mid, new DoubleMatrix(features))
    }
    val movieRecs = mFeatures.cartesian(mFeatures)
      .filter {
        case (m1, m2) => m1._1 != m2._1
      }
      .map {
        case (m1, m2) =>
          val distance: Double = ODist(m1._2, m2._2)
          ((m1._1, (m2._1, distance)))
      }
      .filter(
        m => m._2._2 > 0.6
      )
      .groupByKey()
      .map {
        case (mid, items) => MovieRecs(mid, items.toList.sortWith((m1, m2) => m1._2 > m2._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()
    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
    // 对所有电影两两计算它们的相似度, 先做笛卡尔积


  }

  // 求向量余弦相似度
  def ODist(m1: DoubleMatrix, m2: DoubleMatrix): Double = {
    m1.dot(m2) / (m1.norm1() * m2.norm2())
  }
}

