from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def build_gold_app_achievement_summary(spark):
    silver_df = spark.table("silver.global_achievements")

    gold_df = (
        silver_df
        .groupBy("appid", "game_name")
        .agg(
            F.countDistinct("achievement_name").alias("total_achievements"),
            F.avg("percentage").alias("avg_completion_percent"),
            F.min("percentage").alias("hardest_achievement_percent"),
            F.max("percentage").alias("easiest_achievement_percent"),
            F.first("player_count").alias("player_count"),
        )
    )

    gold_df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("gold.app_achievement_summary")

def build_gold_hardest_achievements(spark):
    silver_df = spark.table("silver.global_achievements")

    gold_df = (
        silver_df
        .filter(F.col("percentage") > 0)
        .orderBy(F.col("percentage").asc())
        .select(
            "appid",
            "game_name",
            "achievement_name",
            "percentage",
            "player_count"
        )
    )

    gold_df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("gold.hardest_achievements")

def build_gold_completion_friendly_games(spark):
    silver_df = spark.table("silver.global_achievements")

    gold_df = (
        silver_df
        .groupBy("appid", "game_name")
        .agg(
            F.avg("percentage").alias("avg_completion_percent"),
            F.countDistinct("achievement_name").alias("achievement_count"),
            F.first("player_count").alias("player_count"),
        )
        .orderBy(F.col("avg_completion_percent").desc())
    )

    gold_df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("gold.completion_friendly_games")
