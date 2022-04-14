from pyspark.sql import SparkSession
import json
from functools import partial
from pyspark.sql.functions import udf, col
from utils.json_utils import parse_json
from schemas.users import USERS_FIELD_DATA_SCHEMA
from schemas.transactions import TRANSACTIONS_FIELD_DATA_SCHEMA


def jsonify_field(x, schema=None):
    data = json.loads(x)
    data = parse_json(data, schema)
    return data


def read_csv(spark_session, file_path, table_name=None):
    transactions = spark_session.read.csv(file_path, quote='"', escape='"', header=True)
    if table_name:
        transactions.createOrReplaceTempView(table_name)
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
