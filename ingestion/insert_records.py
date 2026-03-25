from .clients.factory import get_client

product_data = get_client().fetch_products()
print(f"FETCHED PRODUCT DATA: {product_data}")