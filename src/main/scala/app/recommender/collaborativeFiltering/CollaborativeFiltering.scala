package app.recommender.collaborativeFiltering


import org.apache.spark.rdd.RDD

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = ???

  def predict(userId: Int, movieId: Int): Double = ???

}
