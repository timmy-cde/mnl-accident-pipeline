from pyspark.sql import functions as F
from pyspark.sql.window import Window


def load_vehicles_df(spark, table_id):
    return spark.read.format("bigquery") \
                .option('table', table_id) \
                .load()


def add_vehicle_suggestions(df_involved, df_dim_vehicles):

    verified_lookup = (
        df_dim_vehicles
        .select(
            F.upper(F.trim("vehicle_type")).alias("vehicle_type"),
            "vehicle_group",
            "is_verified"
        )
    )

    # --------------------------------------------------
    # Vehicle types present in staging
    # --------------------------------------------------
    staging_vehicle_types = (
        df_involved
        .select(F.upper(F.trim("vehicle_type")).alias("vehicle_type"))
        .distinct()
    )

    # --------------------------------------------------
    # Verified vehicle types
    # --------------------------------------------------
    verified_types = (
        verified_lookup
        .filter(F.col("is_verified") == True)
        .select(
            F.col("vehicle_type").alias("candidate_vehicle_type"),
            "vehicle_group"
        )
    )

    # --------------------------------------------------
    # Vehicle types not yet verified
    # --------------------------------------------------
    unknown = (
        staging_vehicle_types
        .join(
            verified_types,
            staging_vehicle_types.vehicle_type == verified_types.candidate_vehicle_type,
            "left_anti"
        )
    )

    # --------------------------------------------------
    # Find best suggestion
    # --------------------------------------------------
    candidates = (
        unknown
        .crossJoin(F.broadcast(verified_types))
        .withColumn(
            "distance",
            F.levenshtein("vehicle_type", "candidate_vehicle_type")
        )
        .withColumn(
            "max_len",
            F.greatest(
                F.length("vehicle_type"),
                F.length("candidate_vehicle_type")
            )
        )
        .withColumn(
            "similarity",
            1 - (F.col("distance") / F.col("max_len"))
        )
        .withColumn(
            "contains_bonus",
            F.when(
                F.col("vehicle_type").contains(F.col("candidate_vehicle_type"))
                | F.col("candidate_vehicle_type").contains(F.col("vehicle_type")),
                F.lit(0.15)
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "match_score",
            F.col("similarity") + F.col("contains_bonus")
        )
    )

    w = Window.partitionBy("vehicle_type").orderBy(F.desc("match_score"))

    best_suggestion = (
        candidates
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            "vehicle_type",
            F.col("candidate_vehicle_type").alias("suggested_from")
        )
    )

    # --------------------------------------------------
    # Verified rows
    # --------------------------------------------------
    verified_rows = (
        df_involved
        .withColumn("vehicle_type", F.upper(F.trim("vehicle_type")))
        .join(
            verified_lookup.filter(F.col("is_verified") == True),
            "vehicle_type",
            "inner"
        )
        .withColumn("suggested_from", F.lit('NONE'))
        .select(
            "event_id",
            "vehicle_type",
            "vehicle_count",
            "vehicle_group",
            "suggested_from",
            "is_verified"
        )
    )

    # --------------------------------------------------
    # Unverified rows
    # --------------------------------------------------
    unverified_rows = (
        df_involved
        .withColumn("vehicle_type", F.upper(F.trim("vehicle_type")))
        .join(
            verified_lookup.filter(F.col("is_verified") == True)
                          .select("vehicle_type"),
            "vehicle_type",
            "left_anti"
        )
        .join(best_suggestion, "vehicle_type", "left")
        .join(
            verified_types.select(
                F.col("candidate_vehicle_type").alias("suggested_from"),
                "vehicle_group"
            ),
            "suggested_from",
            "left"
        )
        .withColumn("vehicle_group", F.coalesce("vehicle_group", F.lit("UNKNOWN")))
        .withColumn("is_verified", F.lit(False))
        .select(
            "event_id",
            "vehicle_type",
            "vehicle_count",
            "vehicle_group",
            "suggested_from",
            "is_verified"
        )
    )

    return verified_rows.unionByName(unverified_rows)