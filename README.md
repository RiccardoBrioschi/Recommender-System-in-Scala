# CS460 - Systems for data management and data science
# Individual Project by Riccardo Brioschi

Project done for the course Systems for data management and data science (CS460) at EPFL.
The aim of this project is to implement data processing pipelines over Apache Spark in order to build a movie recommender systems.

# Milestone 1:  Analyzing data with Spark

## Task 1:  Bulk-load data

In order to implement the `load()` methods, we first need to find the path in order to retrieve the data. After importing the data, we convert them in the appropriate RDD format and we cache the results.

## Task 2.1: Pre-process movies and rating data for analytics

In this section, we proceed to implement the `init()` function in the class `SympleAnalytics`. Indeed, we define two data structures (`ratingsGroupedByYearByTitle` and `titlesGroupedById`), containing data grouped by year and title, that are going to be used later.

## Task 2.2: Number of movies rated each year

In order to retrieve the number of movies rated each year, we used the `ratingsGroupedByYearByTitle` RDD, defined in the task above. Since this structure is obtained grouping data on the year, we simply need to compute the length of each obtained iterable.

## Task 2.3: Most popular movie each year

In order to compute the most rated movie each year, we compute a reduce operation on `ratingsGroupedByYearByTitle` to find the movie that received the largest number of rating every year. In order to retrieve the movie titles and other specific information, we perform a join in the final step.

## Task 2.4: Genre of most rated movie each year

In order to retrieve the most rated genre, we need to consider the movies that received the largest number of ratings and then analyze (count) how frequently each genre appears.

## Task 2.5: Most and least popular genre of all time

Similar to the previous task, we need to retrieve the most rated movies in each year and then return the least and most popular (frequent) genres across these movies. As before, the results are obtained using a reduce operation.

## Task 2.6: Get all movies by a specified list of genre

In order to get all the movies by genre, we check if the list of genres assigned to each movie contains the requiredGenrese RDD given as a input argument to the method `getAllMoviesByGenre`.

## Task 2.7: Get all movies by a specified list of genres using Spark Broadcast Variables

In this section we broadcast the requiredGenres variable in order to reduce the amount of data that has to be moved across nodes. This is beneficial in terms of performance.

# Milestone 2: Movie-ratings pipeline

## Task 3.1: Rating aggregation

The `init()` method computes the average rating for each title. In case of missing rating, the default value which is assigned is 0. In order to make the method consistent with the other tasks, we need to consider only the final rating given to a movie by each user. This is accomplished comparing the timestamp of each rating.

## Task 3.2: Ad-hoc keyword rollup

In this task, after considering only the movies containing a set of specific keywords in their genres, we compute the average of the average ratings computed above. Before returning the results, we take care of returning 0.0 if all filtered titles are unrated. Instead, if no title exists for the given query (set of keywords), we return -1.0.

## Task 3.3: Incremental maintenance

Every time the application applies batches of append-only updates, we need to deal with the possibility that a new user has released a new rating for an already rated movie. 
Therefore, if a user has no previous history, we simply increase the number of rating and the cumulative rating a movie has received by considering the new rating by this user. Otherwise, we need to replace the user's oldest rating with the new one, thus increasing the cumulative rating by the difference of the two and without changing the total number of rating the movie has received.
This way of dealing with the old and new rating is implemented in the method `updateResult()`.

# Milestone 3: Prediction Serving

In this task, we have to ensure the application recommends new movies for users to watch.

## Task 4.1: Indexing the dataset

Using an appropriate hash function, we compute the signature for each title and then we cluster titles with the same signature together. This is implemented in the method `getBuckets`, in which we apply minash to the title of each movie to then perform a groupby on the signature.

## Task 4.2: Near-neighbor lookups

Given a list of queries (movie genres), we now want to compute a signature for each query to then perform the clustering (obtained by using a join). This is implemented in both the `lookup()` method in LSHIndex and in the `lookup` method in the class NNLookup.

## Task 4.3: The baseline predictor

The baseline predictor predicts a user’s rating on an unseen movie based on how the movie has been rated by other users. In order to do so, we first need to compute the average rating for each user (user bias), which is later used to preprocess and scale the rating. Since we are working with RDD, the average ratings must be collected as a list in order to avoid having problems with serializability.
After computing the bias, we "normalize" each rating (by subtracting the mean and dividing by a scaling factor) and finally use these processed ratings to return the predictions.

## Task 4.4: Collaborative Filtering

Using Spark's MLlib, we use the alternating least squares algorithm to learn and predict movie ratings for input pairs of the form (movie, user).
After initializing the model (method `init()` in the class Collaborative Filtering) with the correct hyperparameters, we train it and then use it to predict the unknown rating.

## Task 4.5: Making Recommendations

We end our project implementing the `recommendBaseline()` and `recommendCollaborative()` methods in the `Recommender` class.
Both methods take the user ID as input. After considering only the movies which have not been rated by the user, we keep the ones which are related to the `genre` list given as input argument. Once this filtering procedure has been completed (by using `lookup()` and other methods implemented and described above),  the predictions are computed using either the `baselinePredictor` or the `collaborativePredictor`.