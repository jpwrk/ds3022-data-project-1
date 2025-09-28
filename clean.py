import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log",
    filemode="a",  # append to clean.log file
    force=True  # force reconfiguration if logging was already set up
)
logger = logging.getLogger(__name__)


def clean_trips():
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)

        for table in ['yellow_trips', 'green_trips']:
            con.execute(f"""
                CREATE OR REPLACE TABLE {table}_cleaned AS
                SELECT DISTINCT *
                FROM {table}
                WHERE passenger_count > 0
                  AND trip_distance > 0
                  AND trip_distance < 100
                  AND epoch(tpep_dropoff_datetime) - epoch(tpep_pickup_datetime)) < 86400;
            """)
            logger.info(f"Cleaned and de-duplicated {table} into {table}_cleaned")

            # cleaning confirmations
            total_count = con.execute(f"SELECT COUNT(*) FROM {table}_cleaned").fetchone()[0]
            zero_passengers = con.execute(f"SELECT COUNT(*) FROM {table}_cleaned WHERE passenger_count <= 0").fetchone()[0]
            zero_distance = con.execute(f"SELECT COUNT(*) FROM {table}_cleaned WHERE trip_distance <= 0").fetchone()[0]
            over_100 = con.execute(f"SELECT COUNT(*) FROM {table}_cleaned WHERE trip_distance >= 100").fetchone()[0]
            over_day = con.execute(f"""
                SELECT COUNT(*) FROM {table}_cleaned
                WHERE (epoch(tpep_dropoff_datetime) - epoch(tpep_pickup_datetime)) > 86400
            """).fetchone()[0]
            duplicates = con.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT *, COUNT(*) c
                    FROM {table}_cleaned
                    GROUP BY ALL
                    HAVING c > 1
                )
            """).fetchone()[0]

            # Print + Log confirmations
            confirmation = f"""
            ---- {table.upper()} CLEANING REPORT ----
            Total rows: {total_count}
            Zero passengers: {zero_passengers}
            Zero distance: {zero_distance}
            >=100 miles: {over_100}
            >1 day trips: {over_day}
            Duplicates: {duplicates}
            """
            print(confirmation)
            logger.info(confirmation)

    except Exception as e:
        print(f"An error occurred while cleaning: {e}")
        logger.error(f"An error occurred while cleaning: {e}")


if __name__ == "__main__":
    clean_trips()

