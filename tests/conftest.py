import pytest


@pytest.fixture(scope="session")
def data():
    data_ = {
        "business_card": {
            "alternate_mobile_no": "afa",
            "business_name": "afafaf",
            "coordinates": {"lat": 333, "lng": 222},
            "location": "faaf",
            "mobile_no": "afaf",
            "name": "affa",
        },
        "business_name": "afafa",
        "businesss_type": "afaf",
        "cashbook_current_balance": -80,
        "contextID": "afa",
        "current_location": {"latitude": 1223, "longitude": 222},
        "fcm_token": "afaafa",
        "fromNewAPP": True,
        "id": "afafaf",
        "img_base_64": "",
        "img_url": "",
        "is_active": True,
        "user_last_activity": {"_seconds": 1633051286, "_nanoseconds": 907000000},
        "user_signup_date": {"_seconds": 1598745600, "_nanoseconds": 0},
    }
    return data_


@pytest.fixture(scope="session")
def schema():
    schema_ = {
        "id": {"path": ["id"], "type": str},
        "user_last_activity": {"path": ["user_last_activity", "_seconds"], "type": int},
        "user_signup_date": {"path": ["user_signup_date", "_seconds"], "type": int},
        "rating": {"path": ["rating", "stars"], "type": int},
    }
    return schema_
