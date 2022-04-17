USERS_FIELD_DATA_SCHEMA = {
    "id": {"path": ["id"], "dtype": "string"},
    "user_last_activity": {
        "path": ["user_last_activity", "_seconds"],
        "dtype": "int",
    },
    "user_signup_date": {
        "path": ["user_signup_date", "_seconds"],
        "dtype": "int",
    },
    "rating": {"path": ["rating", "stars"], "dtype": "float"},
}
