import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean_test.log'
)
logger = logging.getLogger(__name__)

def clean_tables():
    con = None
    
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute("PRAGMA max_temp_directory_size='15GB';") 
        logger.info("Set max_temp_directory_size to 15GB.")
        
        con.execute("SET memory_limit='16GB';") 
        logger.info("Set memory_limit to 16GB.")
        
        con.execute("SET threads=4;") 
        logger.info("Set threads to 4 for stable processing.")

        # Years to process (adjust to your actual range)
        years = list(range(2015, 2025))  # 2015-2024
        
        for year in years:
            logger.info(f"Processing year {year}")
            
            # Removing trips with no passengers - YEAR BY YEAR
            
            ## Removing yellowtrips with no passengers for this year
            before_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            con.execute(f"""
                DELETE FROM yellow_tripdata  
                WHERE (passenger_count = 0 OR passenger_count IS NULL)
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]
            
            logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with no passengers from yellowtrip table for year {year}")

            ## Removing greentrips with no passengers for this year
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

            # Removing trips with len 0 - YEAR BY YEAR
            
            ## Removing yellowtrips with len 0 for this year
            before_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            con.execute(f"""
                DELETE FROM yellow_tripdata  
                WHERE (distance = 0 OR distance IS NULL)
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with trip len 0 from yellowtrip table for year {year}")

            ## Removing greentrips with len 0 for this year
            before_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            con.execute(f"""
                DELETE FROM green_tripdata  
                WHERE (distance = 0 OR distance IS NULL)
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            logger.info(f"Dropped {before_green_length-after_green_length} rows with trip len 0 from greentrip table for year {year}")

            # Removing trips with len > 100 - YEAR BY YEAR
            
            ## Removing yellowtrips with len > 100 for this year
            before_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            con.execute(f"""
                DELETE FROM yellow_tripdata  
                WHERE distance > 100
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with trip len > 100 from yellowtrip table for year {year}")

            ## Removing greentrips with len>100 for this year
            before_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            con.execute(f"""
                DELETE FROM green_tripdata  
                WHERE distance > 100
                AND EXTRACT(year FROM pickup_datetime) = {year};
            """)
            
            after_green_length = con.execute(f"""
                SELECT COUNT(*) FROM green_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

            logger.info(f"Dropped {before_green_length-after_green_length} rows with trip len>100 from greentrip table for year {year}")

            # Removing trips over one day - YEAR BY YEAR
            
            ## Removing yellowtrips over one day for this year
            before_yellow_length = con.execute(f"""
                SELECT COUNT(*) FROM yellow_tripdata 
                WHERE EXTRACT(year FROM pickup_datetime) = {year};
            """).fetchone()[0]

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

            logger.info(f"Dropped {before_yellow_length-after_yellow_length} rows with trips > 1 day from yellowtrip table for year {year}")

            ## Removing greentrips over one day for this year
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
            
            # Vacuum after each year to clean up deletion vectors
            logger.info(f"Vacuuming tables after processing year {year}")
            con.execute("VACUUM yellow_tripdata;")
            con.execute("VACUUM green_tripdata;")
            
        # Now add your verification queries at the end (same as your original script)
        logger.info("Cleaning complete - running verification checks")
        
        # Your duplicate/verification checks here...
        
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        
if __name__ == "__main__":
    clean_tables()