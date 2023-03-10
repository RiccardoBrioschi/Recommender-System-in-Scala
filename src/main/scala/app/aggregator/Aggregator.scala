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

  private var state : RDD[(Int, String, Double, List[String])] = null
  private var partitioner: HashPartitioner = null

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

    // Compute the average rating for each title
    val titleRatings = ratings
      .map(rating => (rating._2, (rating._4, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues { case (totalRating, count) => totalRating / count }

    // Add the rating to each title and fill in missing ratings with 0.0
    val titledWithRatings = title
      .map(title => (title._1, title))
      .leftOuterJoin(titleRatings).mapValues {case (title,Some(rating)) => (title._2,title._3,rating)
      case (title, None) => (title._2,title._3,0.0) }.map(term => (term._1, term._2._1, term._2._3, term._2._2))

    // Saving everything in variable space
    state = titledWithRatings.persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    // Map on state to have the result in the proper form
    val result = state.map(term => (term._2, term._3))
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

    val result = state.filter( term => keywords.forall(x => term._4.contains(x)))
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
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = ???
}
