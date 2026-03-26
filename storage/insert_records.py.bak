from ingestion.clients.factory import get_client
from storage.connection import connect_to_db
import psycopg2


def insert_product_data(conn, product_data):
    print("INSERTING PRODUCT DATA INTO DATABASE...")
    try:
        cursor = conn.cursor()
        for product in product_data["products"]:
            cursor.execute(
                """
                INSERT INTO raw.products (
                    id, title, description, category, price, discount_percentage, rating, stock, brand, sku, availability_status, minimum_order_quantity
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    product['id'],
                    product['title'],
                    product['description'],
                    product['category'],
                    product['price'],
                    product['discountPercentage'],
                    product['rating'],
                    product['stock'],
                    product['brand'],
                    product['sku'],
                    product['availabilityStatus'],
                    1,  # minimum_order_quantity
                )
            )
        conn.commit()
        cursor.close()
        print("PRODUCT DATA INSERTED SUCCESSFULLY")
    except psycopg2.Error as e:
        print(f"ERROR INSERTING PRODUCT DATA: {e}")
        raise


def main():
    try:
        conn = connect_to_db()
        product_data = get_client().fetch_products()
        print(f"FETCHED PRODUCT DATA: {product_data}")
        insert_product_data(conn, product_data)
    except Exception as e:
        print("AN ERROR OCCURRED DURING THE INSERTION PROCESS")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("DATABASE CONNECTION CLOSED")


main()