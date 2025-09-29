import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)


# Initial files used to load in January 2024, build out schema for tables
yellowtrip_file = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
greentrip_file = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet"

def load_parquet_files():
    """
    Loads yellow and green taxi trip data for years 2015-2024 into DuckDB tables
    Also loads vehicle emissions data from CSV file
    """

    con = None
    # Creating list of months with proper formatting
    all_months = [f"{i:02d}" for i in range(1, 13)] 
    years = list(range(2015, 2024)) # Years 2015 through 2024

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Dropping yellow trip if pre-existing

        ## Checking if yellow trip table exists
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_tripdata;
        """)
        logger.info("Dropped yellow trip table if exists")

        ## Checking if green trip table exists
        con.execute("""
            DROP TABLE IF EXISTS green_tripdata;
        """)
        logger.info("Dropped green trip table if exists")

        con.execute("""
            DROP TABLE IF EXISTS vehicle_emissions;
        """)
        logger.info("Dropped vehicle emissions table if exists")

        # Creating tables

        ## Yellow trip table, only reading in needed variables
        # This originally creates the table with only 2024-01 data.
        con.execute(f"""
                    CREATE TABLE yellow_tripdata AS
                    SELECT
                    --- renaming pickup and dropoff for consistency between tables
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

        ## Carbon Emissions Table, read from local csv file
        con.execute("""
            CREATE TABLE vehicle_emissions
                AS
            SELECT
            vehicle_type,
            co2_grams_per_mile
            FROM read_csv('data/vehicle_emissions.csv');
        """)
        logger.info("Imported emissions csv file to DuckDB table")

        # Loop over files to add all years, skips 01 2024(used to create table originally)
        for year in years:
            for month in all_months:
                # Skip the first month of 2024 since it was used for CREATE TABLE
                if year == 2024 and month == '01':
                    continue

                # URL for parquet files, updated for each month/year for every iteration
                yellow_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
                green_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"

                ## Reading parquet and inserting into yellowtrip table
                con.execute(f"""
                INSERT INTO yellow_tripdata
                SELECT tpep_pickup_datetime, tpep_dropoff_datetime, 
                passenger_count, trip_distance
                FROM read_parquet('{yellow_url}');
                """)
                logger.info(f"Added yellow trip data for {year}-{month}")

                ## Reading parquet and inserting into greentrip table
                con.execute(f"""
                INSERT INTO green_tripdata
                SELECT lpep_pickup_datetime, lpep_dropoff_datetime, 
                passenger_count, trip_distance
                FROM read_parquet('{green_url}');
                """)
                logger.info(f"Added green trip data for {year}-{month}")

                # Breaking for 30 seconds between iterations to not overload
                time.sleep(30)
                logger.info(f'Pausing for 30 seconds after {year}-{month}') 
                                
        ## Counting rows from each table, saving to vars

        # Fetching raw row counts
        yellow_count = con.execute("SELECT COUNT(*) FROM yellow_tripdata").fetchone()[0]
        green_count = con.execute("SELECT COUNT(*) FROM green_tripdata").fetchone()[0]
        vehicle_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        
        # Finding avg distance and passenger numbers 
        yellow_stats = con.execute("""
        SELECT AVG(distance) as avg_distance,
        AVG(passenger_count) as avg_passengers
        FROM yellow_tripdata;
        """).fetchone()

        green_stats = con.execute("""
        SELECT AVG(distance) as avg_distance,
        AVG(passenger_count) as avg_passengers
        FROM green_tripdata;
        """).fetchone()

        # Outputting to console
        print(f"Yellow Trip Rows: {yellow_count:,}")
        print(f"Green Trip Rows: {green_count:,}")
        print(f"Vehicle Emissions Rows: {vehicle_count}")
        print(f"\nYellow Trip Stats - Avg Distance: {yellow_stats[0]:.2f} miles, Avg Passengers: {yellow_stats[1]:.2f}")
        print(f"Green Trip Stats - Avg Distance: {green_stats[0]:.2f} miles, Avg Passengers: {green_stats[1]:.2f}")

        # Logging
        logger.info(f"Yellow trips: {yellow_count:,}, Avg distance: {yellow_stats[0]:.2f}")
        logger.info(f"Green trips: {green_count:,}, Avg distance: {green_stats[0]:.2f}")

    # If an error occurs, saves to log and prints to console
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

# Calls the script to execute
if __name__ == "__main__":
    load_parquet_files()