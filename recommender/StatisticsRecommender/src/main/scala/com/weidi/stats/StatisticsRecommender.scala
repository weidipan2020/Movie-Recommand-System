package com.weidi.stats

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

// TODO: 定义样例类
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {

  // TODO: 定义常量
  //1.1 定义表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //1.2 统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    // set a config
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个SparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommeder")

    // 创建一个SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    // 包装一个隐式配置
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    // 从mongodb加载数据
    val movieDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()
    val ratingDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    // 创建名为ratings的临时表
    ratingDF.createOrReplaceTempView("ratings")

    // TODO: 不同的统计推荐结果
    // 1,    1061,    3.0,     1260759182
    // uid,   mid,    score,    timestamp
    // 1. 历史热门统计, 历史评分数据最多, mid, count
    val mostPopularMoviesDF = sparkSession.sql(
      "select mid, count(mid) as count" +
        "from ratings" +
        "group by mid" +
        "order by count"
    )

    // 把结果写入对应的mongodb表中
    storeDFToMongoDB(mostPopularMoviesDF, RATE_MORE_MOVIES)

    // 2. 近期热门统计, 按照“yyyyMM”格式选取最近的评分数据，统计评分个数
    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册udf，把时间戳转换成年月格式
    sparkSession.udf.register("changeDate", (x: Int) =>
    simpleDateFormat.format(new Date(x * 1000L)).toInt)

    // 对原始数据做预处理，去掉uid
    sparkSession.sql(
      "select mid, score, changeDate(timestamp) as yyyyMM" +
      "from ratings"
    ).createOrReplaceTempView("ratingsInMonth")

    // 从ratingOfMonth中查找电影在各个月份的评分，mid，count，yearmonth
    val recentMostPopularMoviesDF = sparkSession.sql(
      "select mid, count(mid) as count" +
        "from ratingsInMonth" +
        "group by yyyyMM, mid" +
        "order by yyyyMM desc, count desc"
    )

    // 存入mongodb
    storeDFToMongoDB(recentMostPopularMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // 3. 优质电影统计，统计电影的平均评分，mid，avg
    val averageScore = sparkSession.sql(
      "select mid, avg(score) as avg" +
        "from ratings" +
        "group by mid" +
        "order by avg desc")

    storeDFToMongoDB(averageScore, AVERAGE_MOVIES)

    // 4. 各类别电影Top统计
    // 定义所有类别
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    // 把平均评分加入movie表里，加一列，inner join
    val movieWithAvgScore = movieDF.join(averageScore, "mid")

    // 为做笛卡尔积，把genres转成rdd
    val genresRDD = sparkSession.sparkContext.makeRDD(genres)

    // 计算类别top10，首先对类别和电影做笛卡尔积
    // 条件过滤，找出movie的字段genres值(Action|Adventure|Sci-Fi)包含当前类别genre(Action)的那些
    val topGenreMovies = genresRDD.cartesian(movieWithAvgScore.rdd)
      .filter{
        case (genre, movieRow) =>
          movieRow.getAs[String]("genres")
          .toLowerCase
          .contains(genre.toLowerCase)
      }
      .map{
        case (genre, movieRow) =>
          (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
      }
      .groupByKey()
      .map{
        case (genre, items) =>
          GenresRecommendation(genre, items.toList.sortWith(_._2>_._2).take(10).map(item=>Recommendation(item._1, item._2))
      }.toDF()

    storeDFToMongoDB(topGenreMovies, GENRES_TOP_MOVIES)

    sparkSession.stop()
  }

  def storeDFToMongoDB(df: DataFrame, collection : String)(implicit mongoConfig: MongoConfig) = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
