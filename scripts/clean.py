import duckdb
import logging


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Removing duplicate trips

        ### Getting original length(repeated for other steps)
        before_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]

        ## Removing yellowtrip dups
        con.execute("""
                    -- Creating new table w/ only unique rows
                    CREATE TABLE yellow_tripdata_clean AS 
                    SELECT DISTINCT * FROM yellow_tripdata;
                    """)
        logger.info("Created clean yellowtrip table with unique rows")

        con.execute("""
                    -- Dropping original, renaming
                    DROP TABLE yellow_tripdata;
                    ALTER TABLE yellow_tripdata_clean RENAME TO yellow_tripdata;
                    """)
        
        ### Getting length after(repeated for other steps)
        after_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]

        logger.info(f"Dropped {before_yellow_length-after_yellow_length} duplicate rows from yellowtrip table")
        
        ## Removing greentrip dups

        before_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        con.execute("""
                    -- Creating new table w/ only unique rows
                    CREATE TABLE green_tripdata_clean AS 
                    SELECT DISTINCT * FROM green_tripdata;
                    """)
        logger.info("Created clean greentrip table with unique rows")

        con.execute("""
                    -- Dropping original, renaming
                    DROP TABLE green_tripdata;
                    ALTER TABLE green_tripdata_clean RENAME TO green_tripdata;
                    """)
        
        after_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        logger.info(f"Dropped {before_green_length-after_green_length} duplicate rows from greentrip table")

        # Removing trips with no passengers

        ## Removing yellowtrips with no passengers

        before_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]

        con.execute("""
                    DELETE FROM yellow_tripdata  
                    WHERE passenger_count = 0
                    OR passenger_count IS NULL;
                    """)
        
        after_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]
        
        logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with no passengers from yellowtrip table")

        ## Removing greentrips with no passengers

        before_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        con.execute("""
                    DELETE FROM green_tripdata  
                    WHERE passenger_count = 0
                    OR passenger_count IS NULL;
                    """)
        
        after_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        logger.info(f"Dropped {before_green_length-after_green_length} rows with no passengers from greentrip table")

        # Removing trips with len 0

        ## Removing yellowtrips with len 0
        before_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]

        con.execute("""
                    DELETE FROM yellow_tripdata  
                    WHERE distance = 0
                    OR distance IS NULL;
                    """)
        
        after_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]

        logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with trip len 0 from yellowtrip table")

        ## Removing greentrips with len 0
        before_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        con.execute("""
                    DELETE FROM green_tripdata  
                    WHERE distance = 0
                    OR distance IS NULL;
                    """)
        
        after_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        logger.info(f"Dropped {before_green_length-after_green_length} rows with trip len 0 from greentrip table")

        # Removing trips with len > 100

        ## Removing yellowtrips with len > 100
        before_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]

        con.execute("""
                    DELETE FROM yellow_tripdata  
                    WHERE distance > 100;
                    """)
        
        after_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]

        logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with trip len > 100 from yellowtrip table")

        ## Removing greentrips with len>100
        before_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        con.execute("""
                    DELETE FROM green_tripdata  
                    WHERE distance > 100;
                    """)
        
        after_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        logger.info(f"Dropped {before_green_length-after_green_length} rows with trip len>100 from greentrip table")

        # Removing trips over one day

        ## Removing yellowtrips over one day
        before_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]

        con.execute("""
                    DELETE FROM yellow_tripdata  
                    WHERE date_diff('second', pickup_datetime, dropoff_datetime) > 86400
                    OR pickup_datetime IS NULL
                    OR pickup_datetime IS NULL;
                    """)
        
        after_yellow_length = con.execute("""SELECT COUNT(*) FROM yellow_tripdata;""").fetchone()[0]

        logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with trips > 1 day from yellowtrip table")

        ## Removing greentrips with over one day
        before_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        con.execute("""
                    DELETE FROM green_tripdata  
                    WHERE date_diff('second', pickup_datetime, dropoff_datetime) > 86400
                    OR pickup_datetime IS NULL
                    OR pickup_datetime IS NULL;
                    """)
        
        after_green_length = con.execute("""SELECT COUNT(*) FROM green_tripdata;""").fetchone()[0]

        logger.info(f"Dropped {before_green_length-after_green_length} rows with trips>1 day from greentrip table")

        # Checking to make sure all rows were dropped

        ## Checking for duplicate rows

        ### Finding and saving duplicate yellow rows(checking all vars)
        yellow_dups = con.execute("""
                                SELECT COUNT(*) - COUNT(
                                DISTINCT (VendorID, pickup_datetime, dropoff_datetime,
                                passenger_count, distance, pickup_location, dropoff_location))
                                FROM yellow_tripdata;
                                """).fetchone()[0]
        print(f"Yellow trip duplicate rows: {yellow_dups} remaining after clean")
        logger.info(f"Yellow trip duplicate rows: {yellow_dups} remaining after clean")

        ### Finding and saving duplicate green rows(checking all vars)
        green_dups = con.execute("""
                                SELECT COUNT(*) - COUNT(
                                DISTINCT (VendorID, pickup_datetime, dropoff_datetime,
                                passenger_count, distance, pickup_location, dropoff_location))
                                FROM green_tripdata;
                                """).fetchone()[0]
        print(f"Green trip duplicate rows: {green_dups} remaining after clean")
        logger.info(f"Green trip duplicate rows: {green_dups} remaining after clean")


        ## Checking for 0 passenger trips

        ### Calculating and saving yellow trip rows where passengers = 0
        yellow_no_pass = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE passenger_count = 0;
        """).fetchone()[0]
        print(f"Yellow trip rows with 0 passengers: {yellow_no_pass} remaining after clean")
        logger.info(f"Yellow trip rows with 0 passengers: {yellow_no_pass} remaining after clean")

        ### Calculating and saving green trip rows where passengers = 0
        green_no_pass = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE passenger_count = 0;
        """).fetchone()[0]
        print(f"Green trip rows with 0 passengers: {green_no_pass} remaining after clean")
        logger.info(f"Green trip rows with 0 passengers: {green_no_pass} remaining after clean")

        ## Checking for trips of length 0

        ### Calculating and saving yellow trip rows where length = 0
        yellow_no_dist = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE distance = 0;
        """).fetchone()[0]
        print(f"Yellow trip rows with no length: {yellow_no_dist} remaining after clean")
        logger.info(f"Yellow trip rows with no length: {yellow_no_dist} remaining after clean")

        ### Calculating and saving green trip rows where length = 0
        green_no_dist = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE distance = 0;
        """).fetchone()[0]
        print(f"Green trip rows with no length: {green_no_dist} remaining after clean")
        logger.info(f"Green trip rows with no length: {green_no_dist} remaining after clean")

        ## Checking for trips of length > 100

        ### Calculating and saving yellow trip rows where length > 100
        yellow_too_far = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE distance > 100;
        """).fetchone()[0]
        print(f"Yellow trip rows > 100 miles: {yellow_too_far} remaining after clean")
        logger.info(f"Yellow trip rows > 100 miles: {yellow_too_far} remaining after clean")

        ### Calculating and saving green trip rows where length > 100
        green_too_far = con.execute(f"""
            SELECT COUNT(*) FROM green_tripdata
            WHERE distance > 100;
        """).fetchone()[0]
        print(f"Green trip rows > 100: {green_too_far} remaining after clean")
        logger.info(f"Green trip rows > 100: {green_too_far} remaining after clean")

        ## Checking for trips > 1 day

        ### Calculating and saving yellow trips > 1 day
        yellow_too_long = con.execute(f"""
            SELECT COUNT(*) FROM yellow_tripdata
            WHERE date_diff('second', pickup_datetime, dropoff_datetime) > 86400;
        """).fetchone()[0]
        print(f"Yellow trip rows > 1 day: {yellow_too_long} remaining after clean")
        logger.info(f"Yellow trip rows > 1 day: {yellow_too_long} remaining after clean")

        ### Calculating and saving green trips > 1 day
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
    clean_parquet_files()