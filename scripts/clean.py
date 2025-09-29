import duckdb
import logging

# Configure logging to write to clean.log with timestamp, level, and message
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_tables():
    """
    Cleans yellow and green taxi trip tables by removing invalid data.
    Removes: trips with 0 passengers, 0 distance, >100 miles, >24 hours duration.
    Processes year by year to manage memory efficiently.
    """
    con = None
    
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Setting configurations to optimize performance for large datasets
        con.execute("PRAGMA max_temp_directory_size='15GB';") 
        logger.info("Set max_temp_directory_size to 15GB.")
        
        con.execute("SET memory_limit='16GB';") 
        logger.info("Set memory_limit to 16GB.")
        
        con.execute("SET threads=4;") 
        logger.info("Set threads to 4 for stable processing.")

        years = list(range(2015, 2024)) 
        
        # Process each year individually to manage memory usage
        for year in years:
            logger.info(f"Processing year {year}")
            
            # Removing trips with no passengers

            # Count rows before deletion for this specified year
            before_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                --- checks if year = specified year(repeated throughout)
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            # Delete rows where passenger count is 0 or NULL for this year only
            con.execute(f"""
                DELETE FROM yellow_tripdata  
                WHERE (passenger_count = 0 OR passenger_count IS NULL)
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            # Count rows after deletion to calculate rows removed
            after_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]
            
            # Log number of rows removed
            logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with no passengers from yellowtrip table for year {year}")

            ## Repeating for greentrips
            before_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            con.execute(f"""
                DELETE FROM green_tripdata  
                WHERE (passenger_count = 0 OR passenger_count IS NULL)
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            logger.info(f"Dropped {before_green_length-after_green_length} rows with no passengers from greentrip table for year {year}")

            # Removing trips with distance of 0
            
            ## Removing yellowtrips with distance 0 for this year
            before_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            # Delete rows where distance is 0(or negative) or NULL
            con.execute(f"""
                DELETE FROM yellow_tripdata  
                WHERE (distance = 0 OR distance IS NULL
                        OR distance < 0
                        OR distance = 0.0 )
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            # Log how many rows had 0 distance
            logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with trip len 0 from yellowtrip table for year {year}")

            ## Repeating for greentrips
            before_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            con.execute(f"""
                DELETE FROM green_tripdata  
                WHERE (distance = 0 OR distance IS NULL
                        OR distance < 0
                        OR distance = 0.0)
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            logger.info(f"Dropped {before_green_length-after_green_length} rows with trip len 0 from greentrip table for year {year}")

            # Removing trips with distance > 100(for given year)
            
            ## Removing from yellowtrips for distance > 100
            before_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            # Delete unreasonably long distance trips
            con.execute(f"""
                DELETE FROM yellow_tripdata  
                WHERE distance > 100
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            # Log how many rows were dropped for unreasonably long trips
            logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with trip len > 100 from yellowtrip table for year {year}")

            ## Repeating for greentrips
            before_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            con.execute(f"""
                DELETE FROM green_tripdata  
                WHERE distance > 100
                --- entry from early jan 1 wasn't removed
                AND pickup_datetime >= '{year}-01-01 00:00:00'
                AND pickup_datetime < '{year+1}-01-01 00:00:00';;
            """)
            
            after_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            logger.info(f"Dropped {before_green_length-after_green_length} rows with trip len>100 from greentrip table for year {year}")

            # Removing trips > 1 day
            
            ## Removing yellowtrips over one day for this year
            before_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            # Calculate trip duration and remove if > 86400 seconds (24 hours)
            # Also remove if pickup or dropoff times are NULL
            con.execute(f"""
                DELETE FROM yellow_tripdata  
                WHERE (date_diff('second', pickup_datetime, dropoff_datetime) > 86400
                OR pickup_datetime IS NULL
                OR dropoff_datetime IS NULL)
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            # Log how many trips were dropped for having duration > 1 day
            logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with trips > 1 day from yellowtrip table for year {year}")

            ## Repeating for greentrips
            before_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            con.execute(f"""
                DELETE FROM green_tripdata  
                WHERE (date_diff('second', pickup_datetime, dropoff_datetime) > 86400
                OR pickup_datetime IS NULL
                OR dropoff_datetime IS NULL)
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            logger.info(f"Dropped {before_green_length-after_green_length} rows with trips>1 day from greentrip table for year {year}")
        
        # Remove all records outside of daterange
        con.execute("""
        DELETE FROM yellow_tripdata
        WHERE (EXTRACT(YEAR FROM pickup_datetime) < 2015 
        OR EXTRACT(YEAR FROM pickup_datetime) > 2024)
        """)

        con.execute("""
            DELETE FROM green_tripdata 
            WHERE (EXTRACT(YEAR FROM pickup_datetime) < 2015 
            OR EXTRACT(YEAR FROM pickup_datetime) > 2024)
        """)
        logger.info("Removed all records outside of date range")
        # All years processed - now run verification checks
        logger.info("Cleaning complete - running verification checks")
        
        # VERIFICATION SECTION - Checking to ensure all invalid data was removed
    

        ## Verifying 0 passenger trips are removed

        ### Counting yellow trip rows where passengers = 0 (should be 0)
        yellow_no_pass = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE passenger_count = 0;
        """).fetchone()[0]
        
        # Output to console and log - expecting 0
        print(f"Yellow trip rows with 0 passengers: {yellow_no_pass} remaining after clean")
        logger.info(f"Yellow trip rows with 0 passengers: {yellow_no_pass} remaining after clean")

        ### Counting green trip rows where passengers = 0 (should be 0)
        green_no_pass = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE passenger_count = 0;
        """).fetchone()[0]
        print(f"Green trip rows with 0 passengers: {green_no_pass} remaining after clean")
        logger.info(f"Green trip rows with 0 passengers: {green_no_pass} remaining after clean")

        ## Verifying trips with 0 distance are removed

        ### Counting yellow trip rows where distance = 0 (should be 0)
        yellow_no_dist = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE distance = 0;
        """).fetchone()[0]
        
        # Output to console and log - expecting 0
        print(f"Yellow trip rows with no length: {yellow_no_dist} remaining after clean")
        logger.info(f"Yellow trip rows with no length: {yellow_no_dist} remaining after clean")

        ### Counting green trip rows where distance = 0 (should be 0)
        green_no_dist = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE distance = 0;
        """).fetchone()[0]
        print(f"Green trip rows with no length: {green_no_dist} remaining after clean")
        logger.info(f"Green trip rows with no length: {green_no_dist} remaining after clean")

        ## Verifying trips > 100 miles are removed

        ### Counting yellow trip rows where distance > 100 (should be 0)
        yellow_too_far = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE distance > 100;
        """).fetchone()[0]
        
        # Output to console and log - expecting 0
        print(f"Yellow trip rows > 100 miles: {yellow_too_far} remaining after clean")
        logger.info(f"Yellow trip rows > 100 miles: {yellow_too_far} remaining after clean")

        ### Counting green trip rows where distance > 100 (should be 0)
        green_too_far = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE distance > 100;
        """).fetchone()[0]
        print(f"Green trip rows > 100: {green_too_far} remaining after clean")
        logger.info(f"Green trip rows > 100: {green_too_far} remaining after clean")

        ## Verifying trips > 24 hours are removed

        
        ### Counting yellow trips > 24 hours (should be 0)
        yellow_too_long = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE date_diff('second', pickup_datetime, dropoff_datetime) > 86400;
        """).fetchone()[0]

        print(f"Yellow trip rows > 1 day: {yellow_too_long} remaining after clean")
        logger.info(f"Yellow trip rows > 1 day: {yellow_too_long} remaining after clean")

        ### Counting green trips > 24 hours (should be 0)
        green_too_long = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE date_diff('second', pickup_datetime, dropoff_datetime) > 86400;
        """).fetchone()[0]

        print(f"Green trip rows > 1 day: {green_too_long} remaining after clean")
        logger.info(f"Green trip rows > 1 day: {green_too_long} remaining after clean")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
    
if __name__ == "__main__":
    clean_tables()