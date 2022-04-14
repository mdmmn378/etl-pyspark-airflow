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
        "name": ["business_card", "name"],
        "mobile_no": ["business_card", "mobile_no"],
        "alternate_mobile_no": ["business_card", "alternate_mobile_no"],
        "business_name": ["business_name"],
        "businesss_type": ["businesss_type"],
        "cashbook_current_balance": ["cashbook_current_balance"],
        "contextID": ["contextID"],
        "current_location": ["current_location"],
        "fcm_token": ["fcm_token"],
        "fromNewAPP": ["fromNewAPP"],
        "id": ["id"],
        "img_base_64": ["img_base_64"],
        "img_url": ["img_url"],
        "is_active": ["is_active"],
    }
    return schema_
