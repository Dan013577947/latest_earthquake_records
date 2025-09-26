from api_request import mock_fetch_data
from api_request import parse_data
from api_request import fetch_data
import psycopg2
import pandas as pd

def connect_db():
    print("Connecting to database on progress..")
    try:
        conn = psycopg2.connect(
            host='db',
            port=5432,
            dbname='db',
            user='db_user',
            password='db_password'
        )
        conn.commit()
        print("Connecting to database successful")
        return conn
    except psycopg2.Error as e:
        print(f"Error while connecting to database: {e}")
        raise

def create_table(conn):
    print("Creating table on progress..")
    try:
        cursor = conn.cursor()
        
        cursor.execute(
            """
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_earthquake_records(
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                inserted_at TIMESTAMP DEFAULT NOW(),
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                depth_km FLOAT,
                magnitude FLOAT NOT NULL,
                location TEXT,
                CONSTRAINT unique_earthquake UNIQUE(datetime, latitude, longitude, magnitude)
            );
            """
        )
        conn.commit()
        print("Creating Table successful")
    except psycopg2.Error as e:
        print(f"Error while creating table: {e}")
        raise


def pandas_data(data):
    print("Transforming data to pandas on process..")
    try:
        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["datetime"], format="%d %B %Y - %I:%M %p", errors="coerce")
        df["latitude"] = pd.to_numeric(df["latitude"], errors = "coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors = "coerce")
        df["depth(km)"] = pd.to_numeric(df["depth(km)"], errors = "coerce")
        df["magnitude"] = pd.to_numeric(df["magnitude"], errors = "coerce")
      
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        print("Transforming data with pandas successful")
        return df
    except Exception as e:
        print(f"Error while transforming data with pandas: {e}")
        raise




def insert_table(conn, df):
    print("Inserting data on progress..")
    try:
        records = [ 
            (
                row["datetime"],
                row["latitude"],
                row["longitude"],
                row["depth(km)"],
                row["magnitude"],
                row["location"]
            )
            for _,row in df.iterrows()
        ]
        
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO dev.raw_earthquake_records(
                datetime,
                latitude,
                longitude,
                depth_km,
                magnitude,
                location
                
            ) VALUES(%s,%s,%s,%s,%s,%s)
              ON CONFLICT(datetime, latitude, longitude, magnitude) DO NOTHING;
            """,
                records
            )
        conn.commit()
        print("Inserting data on table successful")
    except Exception as e:
        print(f"Error while inserting data on table: {e}")
        raise

def main():
    try:
        conn = connect_db()
        create_table(conn)
        # data = mock_fetch_data()
        data = parse_data(fetch_data())    
        df = pandas_data(data)
        insert_table(conn, df)
    except Exception as e:
        print(f"Error while in main: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database Connection closed")
