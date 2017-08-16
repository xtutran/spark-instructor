package spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{SQLContext, _}

/**
  * Created by tran.xuantu on 12/13/2016.
  */
object MoviesReco extends MainTrait {

  //  1. Load the sample data.
  //  2. Parse the data into the input format for the ALS algorithm.
  //  3. Split the data into two parts: one for building the model and one for testing
  //  the model.
  //  4. Run the ALS algorithm to build/train a user product matrix model.
  //  5. Make predictions with the training data and observe the results.
  //  6. Test the model with the test data.

  override def createJob(args: Array[String], sc: SparkContext): Unit = {

    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 1.
    val ratingText = sc.textFile("data/ml-20m/ratings.csv")
    // Return the first element in this RDD
    println(ratingText.first())
    // header: userId,movieId,rating,timestamp
    val ratings: DataFrame = readCsv(sqlContext, path = "data/ml-20m/ratings.csv").cache()
    ratings.show()

    val movies: DataFrame = readCsv(sqlContext, path = "data/ml-20m/movies.csv").cache()
    movies.show()

    // 1.a. Explore data
    val numRatings = ratings.count()
    println("count number of total ratings: " + numRatings)

    val numMovies = ratings.select('movieId).distinct().count()
    println("count number of movies rated: " + numMovies)

    val numUsers = ratings.select('userId).distinct().count()
    println("count number of users who rated a movie: " + numUsers)

    // 1.b. More advanced
    ratings.registerTempTable("ratings")
    movies.registerTempTable("movies")

    println("Get the max, min ratings along with the count of users who have rated a movie.")
    val result1 = sqlContext.sql(
      """
        |select movies.title, movierates.maxr, movierates.minr, movierates.cntu
        |from
        | (
        |  SELECT ratings.movieId, max(ratings.rating) as maxr,
        |      min(ratings.rating) as minr,count(distinct userId) as cntu
        |  FROM ratings group by ratings.movieId
        |  ) movierates join movies on movierates.movieId=movies.movieId
        |order by movierates.cntu desc
      """.stripMargin)
    result1.show()

    println("Show the top 10 most-active users and how many times they rated a movie")
    val mostActiveUsersSchemaRDD = sqlContext.sql(
      """
        |SELECT ratings.userId, count(*) as ct
        |FROM ratings GROUP BY ratings.userId ORDER BY ct desc limit 10
      """.stripMargin)
    mostActiveUsersSchemaRDD.collect().foreach(println)

    println("Find the movies that user 4169 rated higher than 4")
    val result2 = sqlContext.sql(
      """
        |SELECT ratings.userId, ratings.movieId, ratings.rating, movies.title
        |FROM ratings JOIN movies ON movies.movieId=ratings.movieId
        |WHERE ratings.userId=4169 and ratings.rating > 4
      """.stripMargin)
    result2.show()

    // 2. Using ALS with the Movie Ratings Data
    // create an RDD of Ratings objects
    val ratingsRDD = ratings.map(r => Rating(r.getInt(0), r.getInt(1), r.getDouble(2))).cache()
    // Randomly split ratings RDD into training
    // data RDD (80%) and test data RDD (20%)
    val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)
    val trainingRatingsRDD = splits(0).cache()
    val testRatingsRDD = splits(1).cache()
    val numTraining = trainingRatingsRDD.count()
    val numTest = testRatingsRDD.count()
    println(s"Training: $numTraining, test: $numTest.")
    // build a ALS user product matrix model with rank=20, iterations=10
    val model = new ALS()
      .setRank(20)
      .setIterations(10)
      .run(trainingRatingsRDD)

    // Model validation
    // get user product pair from testRatings
    val testUserProductRDD = testRatingsRDD.map {
      case Rating(user, product, rating) => (user, product)
    }
    println("get predicted ratings to compare to test ratings")
    val predictionsForTestRDD = model.predict(testUserProductRDD)
    predictionsForTestRDD.take(10).foreach(println)

    // prepare predictions for comparison
    val predictionsKeyedByUserProductRDD = predictionsForTestRDD.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }
    // prepare test for comparison
    val testKeyedByUserProductRDD = testRatingsRDD.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }
    //Join the test with predictions
    val testAndPredictionsJoinedRDD = testKeyedByUserProductRDD.join(predictionsKeyedByUserProductRDD)
    println("print the (user, product),( test rating, predicted rating)")
    testAndPredictionsJoinedRDD.take(3).foreach(println)

    val falsePositives = testAndPredictionsJoinedRDD.filter {
      case ((user, product), (ratingT, ratingP)) => ratingT <= 1 && ratingP >=4
    }
    println(s"There were ${falsePositives.count} false positives out of ${testUserProductRDD.count} test ratings.")

    // Evaluate the model using Mean Absolute Error (MAE) between test
    // and predictions
    val meanAbsoluteError = testAndPredictionsJoinedRDD.map {
      case ((user, product), (testRating, predRating)) =>
        val err = testRating - predRating
        Math.abs(err)
    }.mean()
    println("MAE: " + meanAbsoluteError)

  }
}
