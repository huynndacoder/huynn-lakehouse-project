import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Any, List

from config import settings

logger = logging.getLogger(__name__)


def _format_end_date(end_date: str) -> str:
    if not end_date:
        return ""
    if " " in end_date:
        return end_date
    return f"{end_date} 23:59:59"


class DatabaseService:
    def __init__(self):
        try:
            self.engine = create_engine(
                settings.clickhouse_url, pool_pre_ping=True, pool_recycle=300
            )
            logger.info("DatabaseService initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize DatabaseService: {e}")
            raise

    def is_healthy(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    def execute_query(self, sql: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        try:
            df = pd.read_sql(text(sql), con=self.engine, params=params or {})
            return df
        except SQLAlchemyError as e:
            logger.error(f"SQL Execution Error: {e}")
            raise Exception("Database query failed") from e

    def get_dashboard_stats(
        self, start_date: str = None, end_date: str = None, hours_back: int = None
    ) -> Dict[str, Any]:
        where_clauses = []
        if hours_back:
            where_clauses.append(
                f"tpep_pickup_datetime >= now() - INTERVAL {hours_back} HOUR"
            )
        elif start_date:
            where_clauses.append(f"tpep_pickup_datetime >= '{start_date}'")
        if end_date:
            where_clauses.append(
                f"tpep_pickup_datetime <= '{_format_end_date(end_date)}'"
            )
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Simple count query
        sql_count = f"SELECT COUNT(*) as cnt, SUM(total_amount) as rev, AVG(fare_amount) as avg_f FROM lakehouse.taxi_prod t {where_clause}"

        # Active zones - simple distinct count (fast)
        sql_zones = f"SELECT COUNT(DISTINCT pulocationid) as active_zones FROM lakehouse.taxi_prod t {where_clause}"

        # Separate query for top zones
        sql_top = f"""
            SELECT COALESCE(z.Zone, 'Unknown') as zone_name, COUNT(*) as trips, SUM(t.total_amount) as revenue
            FROM lakehouse.taxi_prod t LEFT JOIN lakehouse.taxi_zones z ON t.pulocationid = z.LocationID
            {where_clause} GROUP BY z.Zone ORDER BY trips DESC LIMIT 5
        """

        # Weather
        sql_weather = "SELECT temperature, humidity, precipitation FROM lakehouse.nyc_weather WHERE time <= now() ORDER BY time DESC LIMIT 1"

        try:
            count_df = self.execute_query(sql_count)
            zones_df = self.execute_query(sql_zones)
            top_df = self.execute_query(sql_top)
            weather_df = self.execute_query(sql_weather)
        except Exception as e:
            logger.error(f"Query error: {e}")
            return {
                "total_trips": 0,
                "total_revenue": 0,
                "avg_fare": 0,
                "active_zones": 0,
                "top_zones": [],
                "peak_hour": "N/A",
                "temperature": 0,
                "humidity": 0,
                "precipitation": 0,
            }

        if count_df.empty:
            return {
                "total_trips": 0,
                "total_revenue": 0,
                "avg_fare": 0,
                "active_zones": 0,
                "top_zones": [],
                "peak_hour": "N/A",
                "temperature": 0,
                "humidity": 0,
                "precipitation": 0,
            }

        row = count_df.iloc[0]
        active_zones = (
            int(zones_df.iloc[0]["active_zones"]) if not zones_df.empty else 0
        )

        totals = {
            "total_trips": int(row.get("cnt", 0) or 0),
            "total_revenue": float(row.get("rev", 0) or 0),
            "avg_fare": float(row.get("avg_f", 0) or 0),
            "active_zones": active_zones,
            "top_zones": top_df.to_dict(orient="records") if not top_df.empty else [],
            "peak_hour": "N/A",
        }

        if not weather_df.empty:
            totals["temperature"] = weather_df.iloc[0]["temperature"]
            totals["humidity"] = weather_df.iloc[0]["humidity"]
            totals["precipitation"] = weather_df.iloc[0]["precipitation"]

        return totals

    def get_recent_trips(
        self, limit: int = 50, hours_back: int = 24
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT 
                vendorid,
                tpep_pickup_datetime as pickup_datetime,
                tpep_dropoff_datetime as dropoff_datetime,
                pulocationid as pickup_location_id,
                dolocationid as dropoff_location_id,
                passenger_count,
                trip_distance,
                fare_amount,
                tip_amount,
                total_amount
            FROM lakehouse.taxi_prod
            WHERE tpep_pickup_datetime >= now() - INTERVAL :hours_back HOUR
            ORDER BY tpep_pickup_datetime DESC
            LIMIT :limit
        """
        params = {"limit": limit, "hours_back": hours_back}
        df = self.execute_query(sql, params)
        return df.to_dict(orient="records")

    def get_zone_performance(
        self,
        start_date: str = None,
        end_date: str = None,
        limit: int = 10,
        hours_back: int = None,
        boroughs: list = None,
    ) -> List[Dict[str, Any]]:
        where_clauses = []
        if hours_back:
            where_clauses.append(
                f"tpep_pickup_datetime >= now() - INTERVAL {hours_back} HOUR"
            )
        if start_date:
            where_clauses.append(f"tpep_pickup_datetime >= '{start_date}'")
        if end_date:
            where_clauses.append(
                f"tpep_pickup_datetime <= '{_format_end_date(end_date)}'"
            )
        if boroughs:
            borough_list = "', '".join(boroughs)
            where_clauses.append(f"z.Borough IN ('{borough_list}')")

        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT 
                t.pulocationid as zone_id,
                COALESCE(z.Zone, 'Unknown') as zone_name,
                COALESCE(z.Borough, 'Unknown') as borough,
                COUNT(*) as pickups,
                SUM(t.total_amount) as revenue,
                AVG(t.fare_amount) as avg_fare,
                AVG(t.trip_distance) as distance,
                COUNTIf(t.dolocationid != t.pulocationid) as dropoffs
            FROM lakehouse.taxi_prod t
            LEFT JOIN lakehouse.taxi_zones z ON t.pulocationid = z.LocationID
            {where_clause}
            GROUP BY t.pulocationid, z.Zone, z.Borough
            ORDER BY pickups DESC
            LIMIT :limit
        """
        df = self.execute_query(sql, {"limit": limit})
        return df.to_dict(orient="records")

    def get_time_series(
        self,
        metric: str,
        interval: str,
        start_date: str = None,
        end_date: str = None,
        hours_back: int = None,
    ) -> Dict[str, Any]:
        metric_map = {
            "trip_count": "COUNT(*)",
            "revenue": "SUM(total_amount)",
            "avg_fare": "AVG(fare_amount)",
        }
        sql_metric = metric_map.get(metric, "COUNT(*)")

        interval_map = {
            "hour": "toStartOfHour",
            "day": "toStartOfDay",
            "week": "toStartOfWeek",
        }
        truncate_func = interval_map.get(interval, "toStartOfHour")

        # Build WHERE clause for date filtering
        where_clauses = []
        if hours_back:
            where_clauses.append(
                f"tpep_pickup_datetime >= now() - INTERVAL {hours_back} HOUR"
            )
        elif start_date:
            where_clauses.append(f"tpep_pickup_datetime >= '{start_date}'")
        if end_date:
            where_clauses.append(
                f"tpep_pickup_datetime <= '{_format_end_date(end_date)}'"
            )
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Calculate limit based on date range
        limit = 8760  # 1 year of hourly data as max
        if start_date and end_date:
            from datetime import datetime

            try:
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
                days_diff = (end - start).days + 1
                if interval == "hour":
                    limit = min(days_diff * 24, 8760)
                elif interval == "day":
                    limit = min(days_diff, 365)
                elif interval == "week":
                    limit = min(days_diff // 7 + 1, 52)
            except:
                pass

        sql = f"""
            SELECT 
                formatDateTime({truncate_func}(tpep_pickup_datetime), '%Y-%m-%dT%H:%i:%SZ') as timestamps,
                {sql_metric} as values
            FROM lakehouse.taxi_prod
            {where_clause}
            GROUP BY {truncate_func}(tpep_pickup_datetime)
            ORDER BY timestamps ASC
            LIMIT {limit}
        """
        df = self.execute_query(sql)

        return {
            "timestamps": df["timestamps"].tolist(),
            "values": [float(v) for v in df["values"].tolist()],
            "metric_name": metric,
            "unit": "USD" if metric in ["revenue", "avg_fare"] else "Trips",
            "interval": interval,
        }

    def get_weather_impact(
        self, start_date: str = None, end_date: str = None, hours_back: int = 720
    ) -> List[Dict[str, Any]]:
        where_clauses = []
        if start_date:
            where_clauses.append(f"t.tpep_pickup_datetime >= '{start_date}'")
        if end_date:
            where_clauses.append(
                f"t.tpep_pickup_datetime <= '{_format_end_date(end_date)}'"
            )
        if not where_clauses:
            where_clauses.append(
                f"t.tpep_pickup_datetime >= now() - INTERVAL {hours_back} HOUR"
            )
        where_clause = "WHERE " + " AND ".join(where_clauses)

        sql = f"""
            SELECT 
                toDate(tpep_pickup_datetime) as date,
                formatDateTime(tpep_pickup_datetime, '%H:00') as hour,
                CASE 
                    WHEN w.precipitation > 5 THEN 'Rainy'
                    WHEN w.precipitation > 0 THEN 'Light Rain'
                    WHEN w.temperature < 0 THEN 'Cold'
                    WHEN w.temperature > 30 THEN 'Hot'
                    ELSE 'Clear'
                END as weather_condition,
                round(AVG(w.temperature), 1) as temperature,
                round(AVG(w.humidity), 1) as humidity,
                COUNT(*) as trips,
                round(AVG(t.fare_amount), 2) as avg_fare,
                round(AVG(t.trip_distance), 2) as avg_distance
            FROM lakehouse.taxi_prod t
            LEFT JOIN lakehouse.nyc_weather w 
                ON toStartOfHour(t.tpep_pickup_datetime) = toStartOfHour(w.time)
            {where_clause}
            GROUP BY toDate(tpep_pickup_datetime), 
                formatDateTime(tpep_pickup_datetime, '%H:00'),
                CASE 
                    WHEN w.precipitation > 5 THEN 'Rainy'
                    WHEN w.precipitation > 0 THEN 'Light Rain'
                    WHEN w.temperature < 0 THEN 'Cold'
                    WHEN w.temperature > 30 THEN 'Hot'
                    ELSE 'Clear'
                END
            ORDER BY date DESC, hour DESC
            LIMIT 500
        """
        df = self.execute_query(sql)
        if df.empty:
            sql_fallback = f"""
                SELECT 
                    toDate(tpep_pickup_datetime) as date,
                    formatDateTime(tpep_pickup_datetime, '%H:00') as hour,
                    'Unknown' as weather_condition,
                    20.0 as temperature,
                    50.0 as humidity,
                    COUNT(*) as trips,
                    round(AVG(fare_amount), 2) as avg_fare,
                    round(AVG(trip_distance), 2) as avg_distance
                FROM lakehouse.taxi_prod
                {where_clause}
                GROUP BY toDate(tpep_pickup_datetime), formatDateTime(tpep_pickup_datetime, '%H:00')
                ORDER BY date DESC, hour DESC
                LIMIT 500
            """
            df = self.execute_query(sql_fallback)
        return df.to_dict(orient="records")

    def get_demand_predictions(self) -> List[Dict[str, Any]]:
        sql = """
            SELECT 
                pulocationid as location_id,
                COALESCE(z.Zone, concat('Zone ', toString(pulocationid))) as zone_name,
                now() + INTERVAL 1 HOUR as prediction_hour,
                round(COUNT(*) * 1.1, 0) as predicted_demand,
                round(0.7 + (rand() / 4294967296.0) * 0.25, 3) as confidence_score,
                round(0.9 + (rand() / 4294967296.0) * 0.2, 2) as weather_impact_factor,
                COUNT(*) as historical_avg
            FROM lakehouse.taxi_prod
            LEFT JOIN lakehouse.taxi_zones z ON pulocationid = z.LocationID
            WHERE tpep_pickup_datetime >= now() - INTERVAL 7 DAY
            GROUP BY pulocationid, z.Zone
            ORDER BY predicted_demand DESC
            LIMIT 20
        """
        df = self.execute_query(sql)
        return df.to_dict(orient="records")

    def get_realtime_activity(self) -> List[Dict[str, Any]]:
        sql = """
            SELECT 
                pulocationid as zone_id,
                COALESCE(z.Zone, concat('Zone ', toString(pulocationid))) as zone_name,
                now() as timestamp,
                round(COUNT(*) / 100.0 * 10, 1) as activity_score,
                COUNT(*) as pickup_count,
                round(SUM(total_amount), 2) as revenue_last_hour,
                round(AVG(trip_distance) / 2.0, 1) as avg_wait_time
            FROM lakehouse.taxi_prod
            LEFT JOIN lakehouse.taxi_zones z ON pulocationid = z.LocationID
            WHERE tpep_pickup_datetime >= now() - INTERVAL 1 HOUR
            GROUP BY pulocationid, z.Zone
            ORDER BY pickup_count DESC
            LIMIT 20
        """
        df = self.execute_query(sql)
        return df.to_dict(orient="records")


def get_db_service() -> DatabaseService:
    return DatabaseService()
