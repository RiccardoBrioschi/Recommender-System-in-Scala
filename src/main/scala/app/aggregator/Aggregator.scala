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

    val titleRatings = ratings.groupBy(_._2).mapValues(_.groupBy(_._1)).map(term => (term._1, term._2.
      mapValues(elem => elem.reduce((a, b) => if (a._5 >= b._5) a else b))))
      .map(term => (term._1, term._2.mapValues(elem => (elem._4,1))))
      .flatMapValues(_.toList).map(term => (term._1, (term._2._2)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))


    val titleWithRatings = title.map(term => (term._1, (term._2, term._3)))

    state = titleWithRatings.leftOuterJoin(titleRatings)
    .mapValues { case (title, Some(rating)) => (title._2, title._1, rating._1, rating._2)
    case (title, None) => ((title._2, title._1,0.0, 0))}.map(term =>
      (term._1,term._2._2, term._2._1,term._2._3, term._2._4))

    state.persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    // Map on state to have the result in the proper form
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

    // this must be modified

    val temp_rdd = state.map(term => if (term._5 == 0) (term._1,term._2, 0.0, term._3) else (term._1,term._2, term._4 / term._5.toDouble, term._3))
    val result = temp_rdd.filter( term => keywords.forall(x => term._4.contains(x)))
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

    val update = sc.parallelize(delta_).map{
      case (user, movie, Some(old_rating), new_rating, timestamp) => (movie, new_rating - old_rating, 0)
      case (user, movie, None, new_rating, timestamp) => (movie, new_rating, 1) }
      .map (term => (term._1, (term._2, term._3))).reduceByKey((a,b)=>
      (a._1 + b._1, a._2 + b._2))

    state = state.map(term => (term._1, (term._2, term._3, term._4, term._5))). leftOuterJoin(update)
      .mapValues{ case (old_r, Some(new_r)) => (old_r._1, old_r._2, old_r._3 + new_r._1, old_r._4 + new_r._2)
    case (old_r, None) => (old_r._1, old_r._2, old_r._3, old_r._4)}
      .map(term => (term._1, term._2._1, term._2._2, term._2._3, term._2._4))


    state.persist()
    }

}
