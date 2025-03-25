from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join on UserID
joined_df = posts_df.join(users_df, on="UserID")

# Group by AgeGroup and compute average Likes and Retweets
engagement_df = joined_df.groupBy("AgeGroup").agg(
    avg("Likes").alias("Avg_Likes"),
    avg("Retweets").alias("Avg_Retweets")
).orderBy(col("Avg_Likes").desc())

# Save result
engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)
