package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  // Initializing helpful variable to reduce the computation
  private var partitioner: HashPartitioner = null
  private var state :RDD[(Int, String, List[String],Double, Int)] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {

    // For every movie, we consider the ratings given by each user. For every single user,
    // we only consider the latest rating. This is achieved by grouping by userId and comparing
    // the timestamp of the observed ratings
    val titleRatings = ratings
      .groupBy(_._2)
      .mapValues(_.groupBy(_._1))
      .map(term => (term._1, term._2.mapValues(elem => elem.reduce((a, b) => if (a._5 >= b._5) a else b))))
      .map(term => (term._1, term._2.mapValues(elem => (elem._4,1))))
      .flatMapValues(_.toList).map(term => (term._1, (term._2._2)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    // We map each term in the title (movie) RDD in order to have the movieId as a key
    val titleWithRatings = title.map(term => (term._1, (term._2, term._3)))

    // We perform an outerleftjoin to compute the average rating for every single movie in the title RDD.
    // If a movie has no ratings, then its average rating is automatically set to zero
    state = titleWithRatings
      .leftOuterJoin(titleRatings)
      .mapValues { case (title, Some(rating)) => (title._2, title._1, rating._1, rating._2)
    case (title, None) => ((title._2, title._1,0.0, 0))}
      .map(term => (term._1,term._2._2, term._2._1,term._2._3, term._2._4))

    // We persist the result in memory to reduce the overload
    state.persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {

    // This function is used to retrieve the result of the aggregation performed in init().
    // Each movie is identified by its title.

    // Map on state to have the result in the proper form ( in order to compute the average, we
    // need to divide the sum of ratings by the number of ratings).
    val result = state.map(term => if (term._5 == 0) (term._2, 0.0) else (term._2, term._4 / term._5.toDouble))
    result
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {

    // This function compute the average of the average ratings among all the movies containing keyword
    // in their genre list.

    // From state we compute a provisional rdd containing every movie and the corresponding average rating for
    // each movie
    val temp_rdd = state.map(term => if (term._5 == 0) (term._1,term._2, 0.0, term._3)
    else (term._1,term._2, term._4 / term._5.toDouble, term._3))

    // From temp_rdd we only keep the movies whose genre list contains the required keyword
    val result = temp_rdd.filter( term => keywords.forall(x => term._4.contains(x)))

    // Dealing with the result in order to return it in the correct format (-1 if no movies,
    // 0 if all the movies received 0.0 as average rating, computing the average otherwise)
    if (result.isEmpty()) -1.0
    else {
      val output = result.filter(movie => movie._3 != 0.0)
      if (output.isEmpty()) 0.0
      else {
        output.map(movie=>movie._3).mean()
      }
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {

    // Given the average ratings stored in state (more precisely we have the sum of ratings and their count)
    // we update them every time we receive new ratings given by users. If a user has no previous history
    // (Option[Double] set to None) we simply increase the count and the sum of rating received by
    // the specific movie; if a user has previous history (Option[Double] contains the last rating),
    // the count is not increased and the provisional sum of ratings is updated using new_rating - old rating

    val update = sc.parallelize(delta_).map{
      case (user, movie, Some(old_rating), new_rating, timestamp) => (movie, new_rating - old_rating, 0)
      case (user, movie, None, new_rating, timestamp) => (movie, new_rating, 1) }
      .map (term => (term._1, (term._2, term._3))).reduceByKey((a,b)=>
      (a._1 + b._1, a._2 + b._2))

    // Saving the updated result in state so that the class can keep trace of every update
    state = state
      .map(term => (term._1, (term._2, term._3, term._4, term._5)))
      .leftOuterJoin(update)
      .mapValues{ case (old_r, Some(new_r)) => (old_r._1, old_r._2, old_r._3 + new_r._1, old_r._4 + new_r._2)
    case (old_r, None) => (old_r._1, old_r._2, old_r._3, old_r._4)}
      .map(term => (term._1, term._2._1, term._2._2, term._2._3, term._2._4))

    // We persist the result in memory to reduce the overload
    state.persist()
    }

}
