package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    // saving movieId for movies similar to the list of genres
    val rdd_genre = sc.parallelize(List(genre))
    // I need to collect otherwise no serializable

    val temp = nn_lookup.lookup(rdd_genre)
    temp.collect().foreach(x => println(x))

    val similar_movies = nn_lookup.lookup(rdd_genre).flatMap(term => term._2.map(elem=> elem._1)).collect().toList

    similar_movies.foreach(x => println(x))

    // for every movie, computing the predicted score
    val predictions = similar_movies.map( term => (term, baselinePredictor.predict(userId, term)))

    // retrieving only the k largest
    val result = predictions.sortBy(_._2).take(K)
    result

  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    // saving movieId for movies similar to the list of genres
    val rdd_genre = sc.parallelize(List(genre))
    // I need to collect otherwise no serializable

    val similar_movies = nn_lookup.lookup(rdd_genre).flatMap(term => term._2.map(elem => elem._1)).collect().toList

    // for every movie, computing the predicted score
    val predictions = similar_movies.map(term => (term, collaborativePredictor.predict(userId, term)))

    // retrieving only the k largest
    val result = predictions.sortBy(_._2).reverse.take(K)

    result.foreach(x => println(x))
    result
  }
}
