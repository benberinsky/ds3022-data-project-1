import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

yellowtrip_file = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
greentrip_file = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet"

def load_parquet_files():

    con = None
    months = ["02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Dropping yellow trip if pre-existing

        ## Checking if yellow trip table exists
        con.execute(f"""
            -- SQL goes here
            DROP TABLE IF EXISTS yellow_tripdata;
        """)
        logger.info("Dropped yellow trip table if exists")

        ## Checking if green trip table exists
        con.execute(f"""
            -- SQL goes here
            DROP TABLE IF EXISTS green_tripdata;
        """)
        logger.info("Dropped green trip table if exists")

        con.execute(f"""
            -- SQL goes here
            DROP TABLE IF EXISTS vehicle_emissions;
        """)
        logger.info("Dropped vehicle emissions table if exists")

        # Creating tables

        ## Yellow trip table
        con.execute(f"""
                    CREATE TABLE yellow_tripdata AS
                    SELECT 
                    VendorID,
                    tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    PULocationID,
                    DOLocationID
                    FROM read_parquet('{yellowtrip_file}');
                    """)
        logger.info("Imported yellow trip parquet file to DuckDB table")

        ## Green trip table
        con.execute(f"""
                    CREATE TABLE green_tripdata AS
                    SELECT
                    VendorID,
                    lpep_pickup_datetime, lpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    PULocationID,
                    DOLocationID
                    FROM read_parquet('{greentrip_file}');
                    """)
        logger.info("Imported green trip parquet file to DuckDB table")

        ## Carbon Emissions Table
        con.execute("""
            CREATE TABLE vehicle_emissions
                AS
            SELECT
            vehicle_type,
            co2_grams_per_mile
            FROM read_csv('../data/vehicle_emissions.csv');
        """)
        logger.info("Imported emissions csv file to DuckDB table")


        # Adding new table for specified month
        for i in months:

            ## Adding all months to yellow trip data
            con.execute(f"""
            INSERT INTO yellow_tripdata
            SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, 
            passenger_count, trip_distance, PULocationID, DOLocationID
            FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{i}.parquet');
            """)
            logger.info(f"Added yellow trip data for month {i}")

            ## Adding all months to green trip data
            con.execute(f"""
            INSERT INTO green_tripdata
            SELECT VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, 
            passenger_count, trip_distance, PULocationID, DOLocationID
            FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{i}.parquet');
            """)
            logger.info(f"Added green trip data for month {i}")
            time.sleep(150)

        ## Counting rows
        # Yellow table row count
        yellow_count = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata;
        """).fetchone()[0]

        # Green table row count
        green_count = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata;
        """).fetchone()[0]
        # Vehicle emissions row count
        vehicle_count = con.execute(f"""
            SELECT COUNT(*) FROM vehicle_emissions;
        """).fetchone()[0]

        
        print(f"Yellow Trips: {yellow_count}")
        print(f"Green Trips: {green_count}")
        print(f"Vehicle Emissions Rows: {vehicle_count}")

        logger.info(f"Rows of each table have been logged as yellow: {yellow_count}, green: {green_count}, vehicle: {vehicle_count}")

        # Loop over files to add all years

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()