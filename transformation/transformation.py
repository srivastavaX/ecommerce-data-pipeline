import logging
import pandas as pd
import os


LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger(__name__)


def transform_products(raw_products: list) -> pd.DataFrame:
    logger.info("TRANSFORMING PRODUCTS")

    df = pd.DataFrame(raw_products)

    # camelCase → snake_case
    df = df.rename(columns={
        "discountPercentage": "discount_percentage",
        "availabilityStatus": "availability_status",
        "minimumOrderQuantity": "minimum_order_quantity",
    })

    columns = [
        "id", 
        "title", 
        "description", 
        "category", 
        "price", 
        "discount_percentage", 
        "rating", 
        "stock", 
        "brand", 
        "sku", 
        "availability_status", 
        "minimum_order_quantity"
    ]
    
    df = df[columns]

    df = df.drop_duplicates(subset="id")
    df = df.dropna(subset=["id"])

    # TYPE CASTING
    df["id"] = df["id"].astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["discount_percentage"] = pd.to_numeric(df["discount_percentage"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").astype("Int64")
    df["minimum_order_quantity"] = pd.to_numeric(df["minimum_order_quantity"], errors="coerce").astype("Int64")

    logger.info(f"TRANSFORMED {len(df)} PRODUCTS")
    return df


def transform_users(raw_users: list) -> pd.DataFrame:
    logger.info("TRANSFORMING USERS...")

    df = pd.DataFrame(raw_users)

    # camelCase → snake_case
    df = df.rename(columns={
        "firstName": "first_name",
        "lastName": "last_name",
        "maidenName": "maiden_name",
    })

    if "address" in df.columns:
        address_df = df["address"].apply(pd.Series)[["city", "state", "country"]]
        df = pd.concat([df.drop(columns=["address"]), address_df], axis=1)

    columns = [
        "id",
        "first_name",
        "last_name",
        "maiden_name",
        "age",
        "gender",
        "email",
        "phone",
        "image",
        "city",
        "state",
        "country"
    ]
    
    df = df[columns]

    df = df.drop_duplicates(subset="id")
    df = df.dropna(subset=["id", "email"])

    df["id"] = df["id"].astype(int)
    df["age"] = pd.to_numeric(df["age"], errors="coerce").astype("Int64")

    logger.info(f"TRANSFORMED {len(df)} USERS")
    return df


def transform_carts(raw_carts: list) -> pd.DataFrame:
    logger.info("TRANSFORMING CARTS...")

    df = pd.DataFrame(raw_carts)

    # camelCase → snake_case
    df = df.rename(columns={
        "userId": "user_id",
        "discountedTotal": "discounted_total",
        "totalProducts": "total_products",
        "totalQuantity": "total_quantity"
    })

    columns = [
        "id",
        "user_id",
        "total",
        "discounted_total",
        "total_products",
        "total_quantity"
    ]
    
    df = df[columns]

    df = df.drop_duplicates(subset="id")
    df = df.dropna(subset=["id", "user_id"])

    df["id"] = df["id"].astype(int)
    df["user_id"] = df["user_id"].astype(int)
    df["total"] = pd.to_numeric(df["total"], errors="coerce")
    df["discounted_total"] = pd.to_numeric(df["discounted_total"], errors="coerce")
    df["total_products"] = pd.to_numeric(df["total_products"], errors="coerce").astype("Int64")
    df["total_quantity"] = pd.to_numeric(df["total_quantity"], errors="coerce").astype("Int64")

    logger.info(f"TRANSFORMED {len(df)} CARTS")
    return df


def transform_cart_items(raw_carts: list) -> pd.DataFrame:
    logger.info("TRANSFORMING CART ITEMS...")
    
    items = []
    for cart in raw_carts:
        cart_id = cart.get("id")
        products = cart.get("products", [])

        for item in products:
            items.append({
                "cart_id": cart_id,
                "product_id": item.get("id"),
                "title": item.get("title"),
                "price": item.get("price"),
                "quantity": item.get("quantity"),
                "total": item.get("total"),
                "discounted_total": item.get("discountedTotal"),
                "discounted_percentage": item.get("discountedPercentage"),
            })

    df = pd.DataFrame(items)

    df = df.drop_duplicates(subset=["cart_id", "product_id"])
    df = df.dropna(subset=["cart_id", "product_id"])

    df["cart_id"] = df["cart_id"].astype(int)
    df["product_id"] = df["product_id"].astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["total"] = pd.to_numeric(df["total"], errors="coerce")
    df["discounted_total"] = pd.to_numeric(df["discounted_total"], errors="coerce")
    df["discounted_percentage"] = pd.to_numeric(df["discounted_percentage"], errors="coerce")

    logger.info(f"TRANSFORMED {len(df)} CART ITEMS")
    return df


def transform_all(raw_data: dict) -> dict:
    logger.info("STARTING FULL TRANSFORMATION...")

    result = {
        "products": transform_products(raw_data["products"]),
        "users": transform_users(raw_data["users"]),
        "carts": transform_carts(raw_data["carts"]),
        "cart_items": transform_cart_items(raw_data["carts"]),
    }

    logger.info("FULL TRANSFORMATION COMPLETE.")
    return result