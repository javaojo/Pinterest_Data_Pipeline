from database_utils import AWSDBConnector
from datetime import datetime
from sqlalchemy import text
from time import sleep
import requests
import random
import json
import creds
import uuid

random.seed(100)


class DateTimeEncoder(json.JSONEncoder):
    # Custom JSON encoder for handling datetime objects
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


# Instantiate the AWSDBConnector class
new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            # Query a random row from the 'pinterest_data' table
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)

            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            # Query a random row from the 'geo_data' table
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)

            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            # Query a random row from the 'user_data' table
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                user_result = dict(row._mapping)

            # Send data to Kinesis streams for each topic
            send_data_to_kinesis("streaming-0ae9e110c9db-pin", pin_result, creds.invoke_url_pin)
            send_data_to_kinesis("streaming-0ae9e110c9db-geo", geo_result, creds.invoke_url_geo)
            send_data_to_kinesis("streaming-0ae9e110c9db-user", user_result, creds.invoke_url_user)


def send_data_to_kinesis(topic, data, invoke_url):
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({
        "StreamName": topic,
        "Data": data,
        "PartitionKey": str(uuid.uuid4())}, cls=DateTimeEncoder)

    try:
        response = requests.request("PUT", invoke_url, headers=headers, data=payload)

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


# Main execution when script is run
if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')

# Comments:
# - This script generates random queries to a MySQL database and sends the obtained data to the EC2 Kinesis Server
