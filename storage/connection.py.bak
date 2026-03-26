import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    print("CONNECTING TO DATABASE...")
    
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", 5432),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        # print(conn)
        return conn
    except psycopg2.Error as e:
        print(f"ERROR CONNECTING TO DATABASE: {e}")
        raise

# connect_to_db()