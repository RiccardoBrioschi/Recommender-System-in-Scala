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

    // Saving the list of genres as an RDD to simplify the computation
    val rdd_genre = sc.parallelize(List(genre))
    // We need to extract movies which have not been rated by the user and which are similar to the required genre.
    // In order to do so, we start by saving the movies rated by the user identified by userId
    val movies_rated_by_user = ratings
      .filter(term=> term._1 == userId)
      .map(term => term._2).collect()
    // Saving movieId for movies similar to the list of genres and extracting the ones not rated by the user so far
    val similar_movies = nn_lookup
      .lookup(rdd_genre)
      .flatMap(term => term._2.map(elem => elem._1))
      .collect()
      .toList // Need to collect otherwise no serializable
    val new_movies_to_rate = similar_movies.filter(elem => !movies_rated_by_user.contains(elem))

    // For each resulting movie, we compute the predicted score using baselinePredictor
    val predictions = new_movies_to_rate.map( term => (term, baselinePredictor.predict(userId, term)))

    // We retrieve the K movies having the largest predicted score
    val result = predictions
      .sortBy(_._2)
      .reverse
      .take(K)

    result

  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    // Saving the list of genres as an RDD to simplify the computation
    val rdd_genre = sc.parallelize(List(genre))
    // We need to extract movies which have not been rated by the user and which are similar to the required genre.
    // In order to do so, we start by saving the movies rated by the user identified by userId
    val movies_rated_by_user = ratings
      .filter(term => term._1 == userId)
      .map(term => term._2)
      .collect()
    // Saving movieId for movies similar to the list of genres and extracting the ones not rated by the user so far
    val similar_movies = nn_lookup
      .lookup(rdd_genre)
      .flatMap(term => term._2.map(elem => elem._1))
      .collect()
      .toList // Need to collect otherwise no serializable
    val new_movies_to_rate = similar_movies.filter(elem => !movies_rated_by_user.contains(elem))

    // For each resulting movie, we compute the predicted score using collaborativePredictor
    val predictions = new_movies_to_rate.map(term => (term, collaborativePredictor.predict(userId, term)))

    // We retrieve the K movies having the largest predicted score
    val result = predictions
      .sortBy(_._2)
      .reverse
      .take(K)

    result
  }
}
