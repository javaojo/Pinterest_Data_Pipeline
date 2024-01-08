from database_utils import AWSDBConnector
from sqlalchemy import text
from time import sleep
import requests
import random
import creds
import json

random.seed(100)

new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)

            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)

            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                user_result = dict(row._mapping)

            send_data_to_kafka("0ae9e110c9db.pin", json.dumps({
                "records": [
                    {
                        "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"],
                                  "title": pin_result["title"], "description": pin_result["description"],
                                  "poster_name": pin_result["poster_name"],
                                  "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"],
                                  "is_image_or_video": pin_result["is_image_or_video"],
                                  "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"],
                                  "save_location": pin_result["save_location"], "category": pin_result["category"]}
                    }
                ]
            }))
            send_data_to_kafka("0ae9e110c9db.geo", json.dumps({
                "records": [
                    {
                        "value": {"ind": geo_result["ind"],
                                  "timestamp": geo_result["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                                  "latitude": geo_result["latitude"], "longitude": geo_result["longitude"],
                                  "country": geo_result["country"]}
                    }
                ]
            }))
            send_data_to_kafka("0ae9e110c9db.user", json.dumps({
                "records": [
                    {
                        "value": {"ind": user_result["ind"], "first_name": user_result["first_name"],
                                  "last_name": user_result["last_name"], "age": user_result["age"],
                                  "date_joined": user_result["date_joined"].strftime("%Y-%m-%d %H:%M:%S")}
                    }
                ]
            }))


def send_data_to_kafka(topic, payload):
    invoke_url = creds.invoke_url_pin + topic
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    try:
        response = requests.request("POST", invoke_url, headers=headers, data=payload)
        # Check the HTTP status code
        if response.status_code == 200:
            print("Data sent successfully!\n")
        elif response.status_code == 401:
            print("Status Code: 401 Authentication error. Check your API key or token.\n")
        elif response.status_code == 403:
            print("Status Code: 403 Authorization error. Insufficient permissions.\n")
        elif response.status_code == 404:
            print("Status Code: 404 API endpoint not found.\n")
        elif response.status_code == 500:
            print("Status Code: 500 Internal server error. Check the API server logs.\n")
        else:
            print(f"Unexpected error: {response.status_code}\n")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}\n")


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
