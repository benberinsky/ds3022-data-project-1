import duckdb
import logging
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

def analyze_tables():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Finding largest carbon producing trip 

        ## Largest carbon producing trip(yellow taxi)
        yellow_co2_max = con.execute(f"""
            SELECT MAX(trip_co2_kgs)
            FROM yellow_tripdata_transformed;
            """).fetchone()[0]
        print(f"The highest co2 producing trip produced {yellow_co2_max} KGs of co2")
        logger.info(f"Found max co2 trip for yellow taxis as {yellow_co2_max}")

        ## Largest carbon producing trip(green taxi)
        green_co2_max = con.execute(f"""
            SELECT MAX(trip_co2_kgs)
            FROM green_tripdata_transformed;
            """).fetchone()[0]
        print(f"The highest co2 producing trip produced {green_co2_max} KGs of co2")
        logger.info(f"Found max co2 trip for green taxis as {green_co2_max}")

        # Finding heaviest and lightest carbon producing hours 

        ## Heaviest/lightest co2 hours for yellow trips
        yellow_co2_hours = con.execute(f"""
            SELECT hour_of_day,
            SUM(trip_co2_kgs) as total_co2
            FROM yellow_tripdata_transformed
            GROUP BY hour_of_day
            ORDER BY total_co2 DESC;
            """).fetchall()
                
        # Pulling values from tuples stored in green_co2_hours
        heaviest_hour = yellow_co2_hours[0][0]
        lightest_hour = yellow_co2_hours[-1][0]

        print(f"Heaviest CO₂ producing hour for yellow trips: {heaviest_hour}")
        print(f"Lightest CO₂ producing hour for yellow trips: {lightest_hour}")
        logger.info(f"Recorded heaviest CO₂ producing hour for yellow trips: {heaviest_hour}")
        logger.info(f"Recorded Lightest CO₂ producing hour for yellow trips: {lightest_hour}")

        ## Heaviest/lightest co2 hours for green trips
        green_co2_hours = con.execute(f"""
            SELECT hour_of_day,
            SUM(trip_co2_kgs) as total_co2
            FROM green_tripdata_transformed
            GROUP BY hour_of_day
            ORDER BY total_co2 DESC;
            """).fetchall()
        
        # Pulling values from tuples stored in green_co2_hours
        heaviest_hour = green_co2_hours[0][0]
        lightest_hour = green_co2_hours[-1][0]

        print(f"Heaviest CO₂ producing hour for green trips: {heaviest_hour}")
        print(f"Lightest CO₂ producing hour for green trips: {lightest_hour}")
        logger.info(f"Recorded heaviest CO₂ producing hour for green trips: {heaviest_hour}")
        logger.info(f"Recorded Lightest CO₂ producing hour for green trips: {lightest_hour}")

        # Finding heaviest and lightest carbon producing days of week 

        ## Heaviest/lightest co2 days for yellow trips
        yellow_co2_days = con.execute(f"""
            SELECT day_of_week,
            SUM(trip_co2_kgs) as total_co2
            FROM yellow_tripdata_transformed
            GROUP BY day_of_week
            ORDER BY total_co2 DESC;
            """).fetchall()
                
        # Pulling values from tuples stored in green_co2_days
        heaviest_day = yellow_co2_days[0][0]
        lightest_day = yellow_co2_days[-1][0]

        print(f"Heaviest CO₂ producing day for yellow trips: {heaviest_day}")
        print(f"Lightest CO₂ producing day for yellow trips: {lightest_day}")
        logger.info(f"Recorded heaviest CO₂ producing day for yellow trips: {heaviest_day}")
        logger.info(f"Recorded Lightest CO₂ producing day for yellow trips: {lightest_day}")

        ## Heaviest/lightest co2 days for green trips
        green_co2_days = con.execute(f"""
            SELECT day_of_week,
            SUM(trip_co2_kgs) as total_co2
            FROM green_tripdata_transformed
            GROUP BY day_of_week
            ORDER BY total_co2 DESC;
            """).fetchall()
        
        # Pulling values from tuples stored in green_co2_days
        heaviest_day = green_co2_days[0][0]
        lightest_day = green_co2_days[-1][0]

        print(f"Heaviest CO₂ producing day for green trips: {heaviest_day}")
        print(f"Lightest CO₂ producing day for green trips: {lightest_day}")
        logger.info(f"Recorded heaviest CO₂ producing day for green trips: {heaviest_day}")
        logger.info(f"Recorded Lightest CO₂ producing day for green trips: {lightest_day}")
        
        # Finding heaviest and lightest carbon producing weeks 

        ## Heaviest/lightest co2 weeks for yellow trips
        yellow_co2_weeks = con.execute(f"""
            SELECT week_of_year,
            SUM(trip_co2_kgs) as total_co2
            FROM yellow_tripdata_transformed
            GROUP BY week_of_year
            ORDER BY total_co2 DESC;
            """).fetchall()
                
        # Pulling values from tuples stored in green_co2_hours
        heaviest_week = yellow_co2_weeks[0][0]
        lightest_week = yellow_co2_weeks[-1][0]

        print(f"Heaviest CO₂ producing week for yellow trips: {heaviest_week}")
        print(f"Lightest CO₂ producing week for yellow trips: {lightest_week}")
        logger.info(f"Recorded heaviest CO₂ producing week for yellow trips: {heaviest_week}")
        logger.info(f"Recorded Lightest CO₂ producing week for yellow trips: {lightest_week}")

        ## Heaviest/lightest co2 weeks for green trips
        green_co2_weeks = con.execute(f"""
            SELECT week_of_year,
            SUM(trip_co2_kgs) as total_co2
            FROM green_tripdata_transformed
            GROUP BY week_of_year
            ORDER BY total_co2 DESC;
            """).fetchall()
        
        # Pulling values from tuples stored in green_co2_hours
        heaviest_week = green_co2_weeks[0][0]
        lightest_week = green_co2_weeks[-1][0]

        print(f"Heaviest CO₂ producing week for green trips: {heaviest_week}")
        print(f"Lightest CO₂ producing week for green trips: {lightest_week}")
        logger.info(f"Recorded heaviest CO₂ producing week for green trips: {heaviest_week}")
        logger.info(f"Recorded Lightest CO₂ producing week for green trips: {lightest_week}")

        # Finding heaviest and lightest carbon producing months 

        ## Heaviest/lightest co2 months for yellow trips
        yellow_co2_months = con.execute(f"""
            SELECT month_of_year,
            SUM(trip_co2_kgs) as total_co2
            FROM yellow_tripdata_transformed
            GROUP BY month_of_year
            ORDER BY total_co2 DESC;
            """).fetchall()
                
        # Pulling values from tuples stored in green_co2_hours
        heaviest_month = yellow_co2_months[0][0]
        lightest_month = yellow_co2_months[-1][0]

        print(f"Heaviest CO₂ producing month for yellow trips: {heaviest_month}")
        print(f"Lightest CO₂ producing month for yellow trips: {lightest_month}")
        logger.info(f"Recorded heaviest CO₂ producing month for yellow trips: {heaviest_month}")
        logger.info(f"Recorded Lightest CO₂ producing month for yellow trips: {lightest_month}")

        ## Heaviest/lightest co2 month for green trips
        green_co2_months = con.execute(f"""
            SELECT month_of_year,
            SUM(trip_co2_kgs) as total_co2
            FROM green_tripdata_transformed
            GROUP BY month_of_year
            ORDER BY total_co2 DESC;
            """).fetchall()
        
        # Pulling values from tuples stored in green_co2_hours
        heaviest_month = green_co2_months[0][0]
        lightest_month = green_co2_months[-1][0]

        print(f"Heaviest CO₂ producing month for green trips: {heaviest_month}")
        print(f"Lightest CO₂ producing month for green trips: {lightest_month}")
        logger.info(f"Recorded heaviest CO₂ producing month for green trips: {heaviest_month}")
        logger.info(f"Recorded Lightest CO₂ producing month for green trips: {lightest_month}")

        # Generating plots

        # Extract months (x-axis values)
        months = [row[0] for row in yellow_co2_months]
        # Extract co2 totals (y-axis values)
        co2_totals = [row[1] for row in yellow_co2_months]

        plt.figure(figsize=(10, 6))
        # Create bar chart with yellow cab color
        plt.bar(months, co2_totals, color='#FDB813')

        plt.title('Carbon Emission for Yellow Cabs by Month', fontsize=16, fontweight='bold')

        # Setting axis labels
        plt.xlabel("Month of Year", fontsize=12)
        plt.ylabel("Total CO₂ (millions of kg)", fontsize=12)

        # Setting xticks
        plt.xticks(range(1, 13))

        # Remove spines for cleaner look
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        plt.tight_layout()

        # Save figure to project directory
        plt.savefig("yellow_cabs_co2_by_month.png")


        # Creating greencab plot

        # Extract months (x-axis values)
        months = [row[0] for row in green_co2_months]
        # Extract co2 totals (y-axis values)
        co2_totals = [row[1] for row in green_co2_months]

        plt.figure(figsize=(10, 6))
        # Create bar chart with green cab color
        plt.bar(months, co2_totals, color='#3BB143')

        plt.title('Carbon Emission for Green Cabs by Month', fontsize=16, fontweight='bold')

        # Setting axis labels
        plt.xlabel("Month of Year", fontsize=12)
        plt.ylabel("Total CO₂", fontsize=12)

        # Setting xticks
        plt.xticks(range(1, 13))

        # Remove spines for cleaner look
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        plt.tight_layout()

        # Save figure to project directory
        plt.savefig("green_cabs_co2_by_month.png")


    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    analyze_tables()