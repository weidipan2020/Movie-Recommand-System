package com.weidi.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import java.net.InetAddress

object DataLoader {
  // 定义常量
  val MOVIES_DATA_PATH = "recommender/DataLoader/src/main/resources/movies.csv"
  val RATINGS_DATA_PATH = "recommender/DataLoader/src/main/resources/ratings.csv"
  val TAGS_DATA_PATH = "recommender/DataLoader/src/main/resources/tags.csv"

  /* TODO: Movie Information
  223                        mid: Int
  Clerks (1994)              name: String
  Convenience and video      descri: String
  92 minutes                 timelong: String
  June 29, 1999              shoot: String
  1994                       issue: String
  English                    language: String
  Comedy                     genres: String
  Brian O'Halloran           director: String
  Kevin Smith                actors: String
  */
  case class Movie(mid: Int, name: String, descri: String, timelong: String,
                   shoot: String, issue: String, language: String, genres: String,
                   director: String, actors: String)

  /* TODO: Ratings Information
  1             uid: Int
  1061          mid: Int
  3.0           score: Double
  1260759182    timestamp: Int
   */
  case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

  /* TODO: Tags Information
  15            uid: Int
  100365        mid: Int
  documentary   tag: String
  1425876220    timestamp: Int
   */
  case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

  case class MongoConfig(uri: String, db: String)

  case class ESConfig(httpHosts: String, transportHosts: String, index: String,
                      clustername: String)

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"


  def main(args: Array[String]): Unit = {
    // 准备配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://rs100:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "rs100:9200",
      "es.transportHosts" -> "rs100:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    // 导入数据
    println("========= 读取数据 =========")
    val rawMovieRDD = spark.sparkContext.textFile(MOVIES_DATA_PATH)
    val rawRatingRDD = spark.sparkContext.textFile(RATINGS_DATA_PATH)
    val rawTagRDD = spark.sparkContext.textFile(TAGS_DATA_PATH)

    // 数据处理
    println("========= 处理数据 =========")
    val movieDF = rawMovieRDD.map(
      line => {
        val parts = line.split("\\^")
        Movie(parts(0).toInt, parts(1), parts(2), parts(3), parts(4), parts(5),
          parts(6), parts(7), parts(8), parts(9));
      }
    ).toDF()

    val ratingDF = rawRatingRDD.map(
      line => {
        val parts = line.split(",")
        Rating(parts(0).toInt, parts(1).toInt, parts(2).toDouble, parts(3).toInt)
      }
    ).toDF()
//    ratingDF.show(truncate = false)

    val tagDF = rawTagRDD.map(
      line => {
        val parts = line.split(",")
        Tag(parts(0).toInt, parts(1).toInt, parts(2), parts(3).toInt)
      }
    ).toDF()

    // 将数据保存到MongoDB
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    dataToMongoDB(movieDF, ratingDF, tagDF)
    println("successfully save data to MongoDB>>>>>>>>>>>>")

    /**
      存到ES中
     */
    // 数据预处理
    import org.apache.spark.sql.functions._

    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))

    // 保存数据到ES
    dataToES(movieWithTagsDF)
//    storeDataInES(movieWithTagsDF)
    println("successfully save data to ES>>>>>>>>>>>>>>>>")

    // 关闭资源
    spark.stop()
  }

  def dataToMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // 如已有表先删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // 写入
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()
  }

  def dataToES(movieDF: DataFrame)(implicit esConfig: ESConfig): Unit = {
    // 新建es配置
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clustername).build()

    // 新建一个es客户端
    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    // 先清理遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index))
      .actionGet().isExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }

    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    movieDF.write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }

  def storeDataInES(movieDF: DataFrame)(implicit eSConfig: ESConfig): Unit = {
    // 新建es配置
    val settings: Settings = Settings.builder().put("cluster.name", eSConfig.clustername).build()

    // 新建一个es客户端
    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    // 先清理遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index))
      .actionGet()
      .isExists
    ) {
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }

    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    movieDF.write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_MOVIE_INDEX)
  }
}
