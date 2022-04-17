TRANSACTIONS_FIELD_DATA_SCHEMA = {
    "amount": {"path": ["amount"], "dtype": "float"},
    "creation_timestamp": {"path": ["creation_timestamp", "_seconds"], "dtype": "int"},
    "customer_net_balance_after_transaction": {"path": ["customer_net_balance_after_transaction"], "dtype": "float"},
    "note": {"path": ["note"], "dtype": "string"},
    "transaction_timestamp": {"path": ["transaction_timestamp", "_seconds"], "dtype": "int"},
    "transaction_type": {"path": ["transaction_type"], "dtype": "string"},
    "type": {"path": ["type"], "dtype": "string"},
    "user_id": {"path": ["user_id"], "dtype": "string"},
}