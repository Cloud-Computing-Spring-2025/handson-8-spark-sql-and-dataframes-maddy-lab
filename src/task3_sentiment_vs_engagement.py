from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize sentiment
posts_df = posts_df.withColumn(
    "Sentiment",
    when(col("SentimentScore") > 0.3, "Positive")
    .when(col("SentimentScore") < -0.3, "Negative")
    .otherwise("Neutral")
)

# Group by Sentiment and compute average Likes and Retweets
sentiment_stats = posts_df.groupBy("Sentiment").agg(
    avg("Likes").alias("Avg_Likes"),
    avg("Retweets").alias("Avg_Retweets")
).orderBy(col("Avg_Likes").desc())

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)
