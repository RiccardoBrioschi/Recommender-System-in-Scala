package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  // Initializing useful variables to reduce complexity
  private var stat_for_user : Array[(Int, Double)] = null
  private var data_so_far :RDD[(Int, Int, Option[Double], Double, Int)] = null
  private var normalized_data_so_far :RDD[(Int, Int, Option[Double], Double, Int)] = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    // In this method we precompute useful operations to persist them in memory and avoid computing them
    // every time a user asks for a prediction

    // Computing average prediction for each user and converting it to a list
    stat_for_user = ratingsRDD
      .map(term => (term._1, (term._2, term._3, term._4, term._5)))
      .groupByKey()
      .mapValues(term => term.map(elem=> (elem._3, 1)).reduce((a,b) => (a._1 + b._1, a._2 + b._2)))
      .map( term => (term._1, term._2._1 / term._2._2)).collect()

    // Saving rating RDD to use it later
    data_so_far = ratingsRDD

    // Given the rating RDD, we modify each entry normalizing it. For this end, we need to use the
    // previously computed user mean and the scaling factor as defined in the project description
    normalized_data_so_far = data_so_far
      .map(term => {
      val user_mean = stat_for_user.filter(elem => elem._1 == term._1).head._2
      val deviation = {
        if (term._4 > user_mean) 5 - user_mean
        else if (term._4 < user_mean) user_mean - 1
        else 1}
      (term._1, term._2, term._3, (term._4 - user_mean) / deviation, term._5)})

    // We persist the result in memory to reduce the overload
    normalized_data_so_far.persist()
  }

  def predict(userId: Int, movieId: Int): Double = {

    // We return the predicted rating userId would have given to
    // the movie identified by movieId

    // Computing the average rating for the specific user in order to set the offset for the future prediction
    val user_mean = {
      if (stat_for_user.filter(term => term._1 == userId).isEmpty)
        data_so_far.map(x => x._4).mean()
      else
        stat_for_user.filter(term => term._1 == userId).head._2

    } // extracting the mean of the user

    // Computing the global average deviation for the movie given as input argument
    val rating_for_specific_movie = normalized_data_so_far
      .filter(term => term._2 == movieId)
      .map(elem => (elem._4, 1))
      .reduce((a,b) => (a._1 + b._1, a._2 + b._2))

    val global_average_deviation_for_movie = rating_for_specific_movie._1 / rating_for_specific_movie._2

    // Computing the final scaling factor (it is used only iff the global average deviation for movie is
    // different from zero)
    val final_scaling_factor = {
    if (global_average_deviation_for_movie != 0.0) {
      val temp_sum = user_mean + global_average_deviation_for_movie
      if (temp_sum > user_mean) 5 - user_mean
      else if (temp_sum < user_mean) user_mean - 1
      else 1
    }
    else 0.0}

    // Computing the prediction and returning it
    val result = {
      if (final_scaling_factor == 0.0) user_mean
      else user_mean + global_average_deviation_for_movie*final_scaling_factor
    }

    result
  }
}
