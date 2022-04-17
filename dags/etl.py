
import pendulum
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameStatFunctions as dstat
from pyspark.sql import functions as funcs
import os


from pyspark.sql.functions import (
    udf,
    lit,
    col,
    current_date,
    datediff,
    timestamp_seconds,
    current_timestamp,
)

from schemas.users import USERS_FIELD_DATA_SCHEMA
from schemas.transactions import TRANSACTIONS_FIELD_DATA_SCHEMA

from pipeline.tools import create_df_columns, read_csv, convert_column_to_json



def df_to_parquet(df, file_path):
    df.write.format("parquet").mode("overwrite").save(file_path)

def load_parquet(spark_session, file_path):
    return spark_session.read.parquet(file_path)


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def creditbook_etl_dag():
    spark_session = (
    SparkSession.builder.appName("CreditBook ETL")
    .config("spark.executor.cores", 8)
    .config("spark.task.cpus", 8)
    .config("spark.cores.max", 24)
    .config("spark.driver.extraClassPath", "jars/postgresql-42.3.3.jar")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.instance", 4)
    .config("spark.driver.memory", "8g")
    .config("spark.driver.maxResultSize", "8g")
    .getOrCreate()
)

    @task(multiple_outputs=True)
    def extract():
        users = read_csv(spark_session, "datasets/users.csv", "users")
        users = convert_column_to_json(users, "data", USERS_FIELD_DATA_SCHEMA)
        users = create_df_columns(users, USERS_FIELD_DATA_SCHEMA, "data")
        analytics = read_csv(spark_session, "./datasets/analytics.csv", "analytics")
        transactions = read_csv(
            spark_session, "./datasets/transactions.csv", "transactions"
        )
        transactions = convert_column_to_json(
            transactions, "data", TRANSACTIONS_FIELD_DATA_SCHEMA
        )
        transactions = create_df_columns(
            transactions, TRANSACTIONS_FIELD_DATA_SCHEMA, "data"
        )
        user_parquet_loc = "./datasets/users.parquet"
        analytics_parquet_loc = "./datasets/analytics.parquet"
        transactions_parquet_loc = "./datasets/transactions.parquet"
        df_to_parquet(users, user_parquet_loc)
        df_to_parquet(analytics, analytics_parquet_loc)
        df_to_parquet(transactions, transactions_parquet_loc)
        locs = {"users": user_parquet_loc, "analytics": analytics_parquet_loc, "transactions": transactions_parquet_loc}
        return locs

    @task()
    def transform(locs):
        users = load_parquet(spark_session, locs["users"])
        analytics = load_parquet(spark_session, locs["analytics"])
        transactions = load_parquet(spark_session, locs["transactions"])
        amount_of_debits = (
            transactions.filter(transactions.transaction_type == "debit")
            .groupby("user_id")
            .sum("amount")
            .withColumnRenamed("sum(amount)", "amount_of_debits")
            .cache()
        )
        no_of_debits = (
            transactions.filter(transactions.transaction_type == "debit")
            .groupby("user_id")
            .count()
            .withColumnRenamed("count", "no_of_debits")
            .cache()
        )
        amount_of_credits = (
            transactions.filter(transactions.transaction_type == "credit")
            .groupby("user_id")
            .sum("amount")
            .withColumnRenamed("sum(amount)", "amount_of_credits")
            .cache()
        )
        no_of_credits = (
            transactions.filter(transactions.transaction_type == "debit")
            .groupby("user_id")
            .count()
            .withColumnRenamed("count", "no_of_credits")
            .cache()
        )
        amount_of_total_transactions = (
            transactions.groupby("user_id")
            .sum("amount")
            .withColumnRenamed("sum(amount)", "amount_of_total_transactions")
            .cache()
        )
        no_of_transactions = (
            transactions.groupby("user_id")
            .count()
            .withColumnRenamed("count", "no_of_transactions")
            .cache()
        )
        ratings = (
            users.select(col("id").alias("user_id"), "rating")
            .groupby("user_id")
            .avg("rating")
            .withColumnRenamed("avg(rating)", "rating")
            .cache()
        )
        user_activity = (
            users.select(col("id").alias("user_id"), "user_last_activity")
            .groupby("user_id")
            .agg({"user_last_activity": "max"})
            .withColumnRenamed("max(user_last_activity)", "user_last_activity")
            .cache()
        )
        user_activity = user_activity.withColumn(
            "days_since_last_activity",
            datediff(current_date(), col("user_last_activity").cast("timestamp")),
        )
        user_activity = user_activity.withColumn(
            "created_at", current_timestamp()
        ).cache()
        user_activity = user_activity.withColumn(
            "user_last_activity", col("user_last_activity").cast("timestamp")
        ).cache()
        days_since_signup = (
            users.groupby("id").agg({"user_signup_date": "first"}).cache()
        )
        days_since_signup = days_since_signup.withColumnRenamed(
            "id", "user_id"
        ).withColumnRenamed("first(user_signup_date)", "days_since_signup")
        days_since_signup = days_since_signup.select(
            "user_id",
            (
                datediff(current_date(), col("days_since_signup").cast("timestamp"))
            ).alias("days_since_signup"),
        ).cache()
        user_info = analytics.groupby("user_id").agg(
            {
                "device_language": "first",
                "city_geoIp": "first",
                "app_version": "first",
                "device_model": "first",
            }
        )
        user_info = user_info.withColumnRenamed(
            "first(app_version)", "app_version"
        ).withColumnRenamed("first(device_model)", "phone_model")
        user_info = user_info.withColumnRenamed(
            "first(device_language)", "language"
        ).withColumnRenamed("first(city_geoIp)", "city")
        median_gmv_per_month = (
            transactions.groupby("user_id", funcs.month("timestamp"))
            .agg(funcs.sum("amount").alias("amount"))
            .groupby("user_id")
            .agg(
                funcs.percentile_approx("amount", 0.5).alias(
                    "calculated_fields.median_gmv_per_month"
                )
            )
            .orderBy("user_id")
        )
        median_trans_per_month = (
            transactions.groupby("user_id", funcs.month("timestamp"))
            .agg(funcs.count("amount").alias("amount"))
            .groupby("user_id")
            .agg(
                funcs.percentile_approx("amount", 0.5).alias(
                    "calculated_fields.median_trans_per_month"
                )
            )
            .orderBy("user_id")
        )
        months_transacting = (
            transactions.select("user_id", funcs.month("timestamp").alias("month_no"))
            .groupby("user_id")
            .agg(
                funcs.expr("count(distinct month_no)").alias(
                    "calculated_fields.months_transacting"
                )
            )
            .orderBy("user_id")
        )
        final = (
            amount_of_credits.join(no_of_credits, on="user_id", how="left")
            .join(amount_of_debits, on="user_id", how="left")
            .join(no_of_debits, on="user_id", how="left")
            .join(amount_of_total_transactions, on="user_id", how="left")
            .join(no_of_transactions, on="user_id", how="left")
            .join(ratings, on="user_id", how="left")
            .join(user_activity, on="user_id", how="left")
            .join(days_since_signup, on="user_id", how="left")
            .join(user_info, on="user_id", how="left")
            .join(median_gmv_per_month, on="user_id", how="left")
            .join(median_trans_per_month, on="user_id", how="left")
            .join(months_transacting, on="user_id", how="left")
        )
        final_loc = "./datasets/final.parquet"
        df_to_parquet(final, final_loc)
        return final_loc
        

    @task()
    def load(final_loc):
        final = load_parquet(spark_session=spark_session, file_path=final_loc)
        final.write.format("jdbc").option(
            "url", "jdbc:postgresql://localhost:5432/test"
        ).option("dbtable", "users_dw").option("user", "mamun").option(
            "password", "mamun1234"
        ).mode(
            "append"
        ).save()

    load(transform(extract()))



etl_output = creditbook_etl_dag()