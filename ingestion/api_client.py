import requests
import logging
# from config.settings import API_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

API_URL = "https://dummyjson.com"

def fetch_data(endpoint, timeout=10):
    url = f"{API_URL}/{endpoint}?limit=100&skip=0"
    
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        records = data.get(endpoint, data)
        logger.info(f"{endpoint.upper()} FETCHED | RECORDS: {len(records)} | STATUS: {response.status_code}")

        return records
        # print(f"API RESPONSE: {response.json()}")
    
    except requests.exceptions.Timeout:
        logger.error(f"REQUEST TIMEOUT: {url}")
        raise

    except requests.RequestException as e:
        logger.error(f"ERROR FETCHING {endpoint.upper()}: {e}")
        raise

def fetch_products():
    return fetch_data("products")

def fetch_users():
    return fetch_data("users")

def fetch_carts():
    return fetch_data("carts")

def fetch_all():
    logger.info("STARTING FULL INGESTION...")
    raw_data = {
        "products": fetch_products(),
        "users": fetch_users(),
        "carts": fetch_carts(),
    }

    logger.info(
        f"INGESTION COMPLETE |"
        f"PRODUCTS={len(raw_data['products'])} |"
        f"USERS={len(raw_data['users'])} |"
        f"CARTS={len(raw_data['carts'])}"
    )

    return raw_data