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
    # Original 'months' list is now used to iterate over months 02-12 for each year.
    # The '01' month is handled by the initial CREATE TABLE statements.
    all_months = [f"{i:02d}" for i in range(1, 13)] # '01', '02', ..., '12'
    years = list(range(2015, 2025)) # Years 2015 through 2024

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
        con.execute("""
            -- SQL goes here
            DROP TABLE IF EXISTS green_tripdata;
        """)
        logger.info("Dropped green trip table if exists")

        con.execute("""
            -- SQL goes here
            DROP TABLE IF EXISTS vehicle_emissions;
        """)
        logger.info("Dropped vehicle emissions table if exists")

        # Creating tables

        ## Yellow trip table
        # This originally creates the table with only 2024-01 data.
        con.execute(f"""
                    CREATE TABLE yellow_tripdata AS
                    SELECT
                    tpep_pickup_datetime AS pickup_datetime, 
                    tpep_dropoff_datetime AS dropoff_datetime,
                    passenger_count,
                    trip_distance AS distance
                    FROM read_parquet('{yellowtrip_file}');
                    """)
        logger.info("Imported yellow trip parquet file to DuckDB table")

        ## Green trip table
        # This originally creates the table with only 2024-01 data.
        con.execute(f"""
                    CREATE TABLE green_tripdata AS
                    SELECT 
                    lpep_pickup_datetime AS pickup_datetime, 
                    lpep_dropoff_datetime AS dropoff_datetime,
                    passenger_count,
                    trip_distance AS distance
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
            FROM read_csv('data/vehicle_emissions.csv');
        """)
        logger.info("Imported emissions csv file to DuckDB table")


        # Adding new table for specified month
        
        # New loop structure to iterate over all years 2015-2024
        # We start by inserting *all* months for *all* years *except* 2024-01, 
        # which was already loaded above to create the table.
        
        # Loop over files to add all years
        for year in years:
            for month in all_months:
                # Skip the first month of 2024 since it was used for CREATE TABLE
                if year == 2024 and month == '01':
                    continue

                yellow_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
                green_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"

                ## Adding all months to yellow trip data
                con.execute(f"""
                INSERT INTO yellow_tripdata
                SELECT tpep_pickup_datetime, tpep_dropoff_datetime, 
                passenger_count, trip_distance
                FROM read_parquet('{yellow_url}');
                """)
                logger.info(f"Added yellow trip data for {year}-{month}")

                ## Adding all months to green trip data
                con.execute(f"""
                INSERT INTO green_tripdata
                SELECT lpep_pickup_datetime, lpep_dropoff_datetime, 
                passenger_count, trip_distance
                FROM read_parquet('{green_url}');
                """)
                logger.info(f"Added green trip data for {year}-{month}")
                time.sleep(30)
                
            # Time delay is kept as per the original script, though it may not be necessary for stability.
            #time.sleep(60) # Sleeps one minute after all 12 months for the current year have been loaded.
            logger.info(f"Pausing for 60 seconds after completing year {year}.")
                                
        ## Counting rows
        # Yellow table row count
        yellow_count = con.execute("""
            SELECT COUNT(*) FROM yellow_tripdata;
        """).fetchone()[0]

        # Green table row count
        green_count = con.execute("""
            SELECT COUNT(*) FROM green_tripdata;
        """).fetchone()[0]
        # Vehicle emissions row count
        vehicle_count = con.execute("""
            SELECT COUNT(*) FROM vehicle_emissions;
        """).fetchone()[0]

        
        print(f"Yellow Trip Rows: {yellow_count}")
        print(f"Green Trip Rows: {green_count}")
        print(f"Vehicle Emissions Rows: {vehicle_count}")

        logger.info(f"Rows of each table have been logged as yellow: {yellow_count}, green: {green_count}, vehicle: {vehicle_count}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()