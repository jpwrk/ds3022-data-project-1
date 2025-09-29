import duckdb
import logging
import sys
import matplotlib.pyplot as plt

# ----------------------------
# Logging Configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log",
    force=True
)
logger = logging.getLogger(__name__)


def run_analysis():
    con = None
    try:
        # Connect to DuckDB
        con = duckdb.connect(database="emissions.duckdb", read_only=True)
        logger.info("Connected to DuckDB (read-only)")

        cab_types = {
            "yellow": "yellow_trips_transform",
            "green": "green_trips_transform"
        }

        results = {}

        for cab, table in cab_types.items():
            logger.info(f"Starting analysis for {cab} taxis ({table})")

            # Largest trip
            try:
                largest_trip = con.execute(f"""
                    SELECT trip_co2_kgs
                    FROM {table}
                    ORDER BY trip_co2_kgs DESC
                    LIMIT 1;
                """).fetchone()[0]
                logger.info(f"Largest CO2 trip for {cab}: {largest_trip:.2f} kgs")
            except Exception as e:
                logger.error(f"Failed largest trip query for {cab}: {e}")

            # Heaviest & lightest hours
            try:
                hours = con.execute(f"""
                    SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
                    FROM {table}
                    GROUP BY hour_of_day
                    ORDER BY avg_co2 DESC;
                """).fetchdf()
                heaviest_hour = int(hours.iloc[0]["hour_of_day"])
                lightest_hour = int(hours.iloc[-1]["hour_of_day"])
                logger.info(f"{cab}: heaviest hour={heaviest_hour}, lightest hour={lightest_hour}")
            except Exception as e:
                logger.error(f"Failed hour query for {cab}: {e}")

            # Heaviest & lightest days
            try:
                days = con.execute(f"""
                    SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
                    FROM {table}
                    GROUP BY day_of_week
                    ORDER BY avg_co2 DESC;
                """).fetchdf()
                heavy_day = int(days.iloc[0]["day_of_week"])
                light_day = int(days.iloc[-1]["day_of_week"])
                logger.info(f"{cab}: heaviest day={heavy_day}, lightest day={light_day}")
            except Exception as e:
                logger.error(f"Failed day query for {cab}: {e}")

            # Heaviest & lightest weeks
            try:
                weeks = con.execute(f"""
                    SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2
                    FROM {table}
                    GROUP BY week_of_year
                    ORDER BY avg_co2 DESC;
                """).fetchdf()
                heavy_week = int(weeks.iloc[0]["week_of_year"])
                light_week = int(weeks.iloc[-1]["week_of_year"])
                logger.info(f"{cab}: heaviest week={heavy_week}, lightest week={light_week}")
            except Exception as e:
                logger.error(f"Failed week query for {cab}: {e}")

            # Heaviest & lightest months
            try:
                months = con.execute(f"""
                    SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2
                    FROM {table}
                    GROUP BY month_of_year
                    ORDER BY avg_co2 DESC;
                """).fetchdf()
                heavy_month = int(months.iloc[0]["month_of_year"])
                light_month = int(months.iloc[-1]["month_of_year"])
                logger.info(f"{cab}: heaviest month={heavy_month}, lightest month={light_month}")
            except Exception as e:
                logger.error(f"Failed month query for {cab}: {e}")

            # Yearly totals for plotting (spanning 2015-2025 only)
            try:
                yearly_totals = con.execute(f"""
                    SELECT year, SUM(trip_co2_kgs) AS total_co2
                    FROM {table}
                    WHERE year BETWEEN 2015 AND 2025
                    GROUP BY year
                    ORDER BY year;
                """).fetchdf()
                results[cab] = yearly_totals
                logger.info(f"Computed yearly totals for {cab} (filtered 2015-2025)")
            except Exception as e:
                logger.error(f"Failed yearly totals query for {cab}: {e}")

        # Plot after both cab types processed
        if results:
            try:
                plt.figure(figsize=(12, 8))
                for cab, data in results.items():
                    plt.plot(data["year"], data["total_co2"], marker="o", label=f"{cab.capitalize()} Taxis", linewidth=2, markersize=6)
                plt.title("Total CO2 Emissions by Year (2015-2025)", fontsize=16, fontweight='bold')
                plt.xlabel("Year", fontsize=14)
                plt.ylabel("Total CO2 Emissions (kgs)", fontsize=14)
                plt.legend(fontsize=12)
                plt.grid(True, alpha=0.3)
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig("co2_emissions_by_year.png", dpi=300, bbox_inches='tight')
                logger.info("Saved plot: co2_emissions_by_year.png")
            except Exception as e:
                logger.error(f"Plotting failed: {e}")

        return con

    except Exception as e:
        logger.error(f"Fatal error in run_analysis: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run_analysis()
