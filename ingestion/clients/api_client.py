import requests

api_url = "https://dummyjson.com/products"
def fetch_products():
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        print(f"API RESPONSE STATUS CODE: {response.status_code} ({response.reason})")
        return response.json()
        # print(f"API RESPONSE: {response.json()}")
    except requests.RequestException as e:
        print(f"ERROR FETCHING DATa: {e}")
        raise