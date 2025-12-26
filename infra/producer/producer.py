import time
import json
import requests
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic  # Added Admin imports
from kafka.errors import TopicAlreadyExistsError

# Define variables for API
API_KEY = "api-key"
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
TOPIC_NAME = "stock-quotes"
BOOTSTRAP_SERVERS = ["host.docker.internal:29092"]


# --- NEW: Auto-creation Logic ---
def create_topic_if_not_exists():
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id='stock_admin'
    )

    topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{TOPIC_NAME}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC_NAME}' already exists, skipping creation.")
    except Exception as e:
        print(f"Error checking/creating topic: {e}")
    finally:
        admin_client.close()


# Run the creation check
create_topic_if_not_exists()

# Initial Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# Retrieve Data
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None


# Looping and Pushing to Stream
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {quote}")
            producer.send(TOPIC_NAME, value=quote)
    time.sleep(6)