from pipeline.extract import *

spark_session = SparkSession.builder.appName("Test").getOrCreate()


if __name__ == '__main__':
    transactions = read_csv(spark_session, "./datasets/transactions.csv", "transactions")
    transactions = convert_column_to_json(transactions, "data", TRANSACTIONS_FIELD_DATA_SCHEMA)
    transactions = create_df_columns(transactions, TRANSACTIONS_FIELD_DATA_SCHEMA, "data")

    transactions.show()