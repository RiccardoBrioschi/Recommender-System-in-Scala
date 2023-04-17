package app.recommender.collaborativeFiltering


import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    // Initializing the ALS object with the correct hyperparameters
    val temp_model = new ALS()
      .setSeed(seed)
      .setRank(rank)
      .setIterations(maxIterations)
      .setLambda(regularizationParameter)
      .setBlocks(n_parallel)

    // Converting the rating in ratingsRDD to the correct format to be used by temp_model
    val correct_rating = ratingsRDD.map(term => new Rating(term._1, term._2, term._4))

    // We save the trained model in model variable. From now on we are going to use it in order to
    // return predictions
    model = temp_model.run(correct_rating)
  }


  def predict(userId: Int, movieId: Int): Double = {

    // Using the model trained in init(), we return the predicted rating userId would have given to
    // the movie identified by movieId
    model.predict(userId, movieId)
  }

}
