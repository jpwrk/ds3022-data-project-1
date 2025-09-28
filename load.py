import duckdb
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log',
    force=True
)
logger = logging.getLogger(__name__)

def load_selected_columns():
    con = None
    try:
        # Connect to DuckDB
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        years = range(2015, 2025)

        yellow_tripdata = [
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
            for year in years for month in range(1, 13)
        ]

        con.execute("DROP TABLE IF EXISTS yellow_trips;")
        con.execute(f"""
            CREATE TABLE yellow_trips AS
            SELECT
                trip_distance,
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count
            FROM read_parquet('{yellow_tripdata[0]}');
        """)
        logger.info("Created yellow_trips with selected columns")

        time.sleep(30)  # avoid throttling

        for url in yellow_tripdata[1:]:
            try:
                con.execute(f"""
                    INSERT INTO yellow_trips
                    SELECT
                        trip_distance,
                        tpep_pickup_datetime,
                        tpep_dropoff_datetime,
                        passenger_count
                    FROM read_parquet('{url}');
                """)
                logger.info(f"Inserted {url} into yellow_trips")
                time.sleep(30)
            except Exception as e:
                logger.error(f"Failed to insert {url}: {e}")

        
        
        
        green_tripdata = [
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
            for year in years for month in range(1, 13)
        ]

        con.execute("DROP TABLE IF EXISTS green_trips;")
        con.execute(f"""
            CREATE TABLE green_trips AS
            SELECT
                trip_distance,
                lpep_pickup_datetime AS tpep_pickup_datetime,
                lpep_dropoff_datetime AS tpep_dropoff_datetime,
                passenger_count
            FROM read_parquet('{green_tripdata[0]}');
        """)
        logger.info("Created green_trips with selected columns")

        time.sleep(30)

        for url in green_tripdata[1:]:
            try:
                con.execute(f"""
                    INSERT INTO green_trips
                    SELECT
                        trip_distance,
                        lpep_pickup_datetime AS tpep_pickup_datetime,
                        lpep_dropoff_datetime AS tpep_dropoff_datetime,
                        passenger_count
                    FROM read_parquet('{url}');
                """)
                logger.info(f"Inserted {url} into green_trips")
                time.sleep(30)
            except Exception as e:
                logger.error(f"Failed to insert {url}: {e}")

        # ------------------------
        # Load Vehicle Emissions
        # ------------------------
        con.execute("DROP TABLE IF EXISTS vehicle_emissions;")
        con.execute("""
            CREATE TABLE vehicle_emissions AS
            SELECT *
            FROM read_csv_auto('data/vehicle_emissions.csv', header=True);
        """)
        logger.info("Loaded vehicle_emissions")

        # ------------------------
        # Row Counts
        # ------------------------
        for table in ["yellow_trips", "green_trips", "vehicle_emissions"]:
            count = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            print(f"{table} row count: {count}")
            logger.info(f"{table} row count: {count}")

        return con

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_selected_columns()
