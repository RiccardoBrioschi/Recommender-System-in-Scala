package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = ???

  def predict(userId: Int, movieId: Int): Double = ???
}
