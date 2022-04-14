from pyspark.sql import SparkSession
import pandas as pd
import json
from functools import partial

from pyspark.sql.functions import udf, lit, col, to_utc_timestamp, to_timestamp
from pyspark.sql.types import FloatType, IntegerType, StringType, TimestampType
from schemas.users import USERS_FIELD_DATA_SCHEMA


from utils.json_utils import parse_json
from schemas.users import USERS_FIELD_DATA_SCHEMA
from schemas.transactions import TRANSACTIONS_FIELD_DATA_SCHEMA


spark_session = SparkSession.builder.appName("test").getOrCreate()

def jsonify_field(x, schema=None):
    data = json.loads(x)
    data = parse_json(data, schema)
    return data


# transactions = spark_session.read.csv("datasets/transactions.csv", quote="\"", escape="\"", header=True)
# transactions.createOrReplaceTempView("transactions")

# transactions = transactions.withColumn("data", udf(transactions_data_field)(col("data")))
# transactions.show()

def read_transactions(spark_session, file_path):
    transactions = spark_session.read.csv(file_path, quote="\"", escape="\"", header=True)
    transactions.createOrReplaceTempView("transactions")
    return transactions


def convert_column_to_json(df, column_name, schema):
    func = partial(jsonify_field, schema=schema)
    return df.withColumn(column_name, udf(func)(col(column_name)))


def extract_field(x, field):
    return x.get(field)


def create_df_columns(df, schema, column_name):
    for key, val in schema.items():
        dtype = val["dtype"]
        field_udf = udf(partial(extract_field, field=key))
        df = df.withColumn(key, field_udf(col(column_name)))
        df = cast_column(df, key, dtype)
    return df

def cast_column(df, column_name, dtype):
    return df.withColumn(column_name, col(column_name).cast(dtype))

def convert_columns(df, column_name, fun):
    return df.withColumn(column_name, fun(col(column_name)))


if __name__ == "__main__":

    users = read_transactions(spark_session, "datasets/users.csv")
    users = convert_column_to_json(users, "data", USERS_FIELD_DATA_SCHEMA)
    users = create_df_columns(users, USERS_FIELD_DATA_SCHEMA, "data")

    users.show()

    # transactions = read_transactions(spark_session, "datasets/transactions.csv")
    # transactions = convert_column_to_json(transactions, "data", TRANSACTIONS_FIELD_DATA_SCHEMA)
    # transactions = create_df_columns(transactions, TRANSACTIONS_FIELD_DATA_SCHEMA, "data")

    # transactions.show()