import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Any, List
import os
import threading

from config import settings
from cache import get_cache_service, cached

logger = logging.getLogger(__name__)


def _clean_for_json(data):
    """Replace NaN/Infinity values in a dictionary for JSON serialization."""
    if not isinstance(data, dict):
        return data
    result = {}
    for k, v in data.items():
        if v is None:
            result[k] = None
        elif isinstance(v, float):
            if np.isnan(v) or np.isinf(v):
                result[k] = None
            else:
                result[k] = v
        elif isinstance(v, list):
            result[k] = [
                _clean_for_json(item) if isinstance(item, dict) else item for item in v
            ]
        else:
            result[k] = v
    return result


def _format_end_date(end_date: str) -> str:
    if not end_date:
        return ""
    if " " in end_date:
        return end_date
    return f"{end_date} 23:59:59"


class DatabaseService:
    """ClickHouse database service with connection pooling and caching."""

    def __init__(self):
        try:
            self.engine = create_engine(
                settings.clickhouse_url,
                pool_size=settings.CH_POOL_SIZE,
                max_overflow=settings.CH_MAX_OVERFLOW,
                pool_timeout=settings.CH_POOL_TIMEOUT,
                pool_recycle=settings.CH_POOL_RECYCLE,
                pool_pre_ping=settings.CH_POOL_PRE_PING,
                pool_use_lifo=True,  # Reuse most recent connections
            )
            self.cache = get_cache_service()
            logger.info(
                f"DatabaseService initialized (pool_size={settings.CH_POOL_SIZE}, "
                f"max_overflow={settings.CH_MAX_OVERFLOW}, cache={'enabled' if self.cache and self.cache.enabled else 'disabled'})"
            )
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

    def get_pool_metrics(self) -> Dict[str, int]:
        """Get connection pool metrics for monitoring."""
        pool = self.engine.pool
        return {
            "pool_size": pool.size(),
            "checked_in_connections": pool.checkedin(),
            "checked_out_connections": pool.checkedout(),
            "overflow_connections": pool.overflow(),
        }

    def invalidate_cache(self, pattern: str = None):
        """Invalidate cached results for a pattern."""
        if not self.cache:
            return
        pattern = pattern or "dashboard:*"
        return self.cache.delete_pattern(pattern)

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

        sql_count = f"SELECT COUNT(*) as cnt, SUM(total_amount) as rev, AVG(fare_amount) as avg_f FROM lakehouse.taxi_prod t {where_clause}"
        sql_zones = f"SELECT COUNT(DISTINCT pulocationid) as active_zones FROM lakehouse.taxi_prod t {where_clause}"
        sql_top = f"""
            SELECT 
                IF(z.Zone = '' OR z.Zone IS NULL, 'Zone ' || toString(t.pulocationid), z.Zone) as zone_name,
                COUNT(*) as trips,
                SUM(t.total_amount) as revenue
            FROM lakehouse.taxi_prod t 
            LEFT JOIN lakehouse.taxi_zones z ON t.pulocationid = z.LocationID
            {where_clause} 
            GROUP BY t.pulocationid, z.Zone 
            ORDER BY trips DESC 
            LIMIT 5
        """
        sql_weather = "SELECT temperature, humidity, precipitation FROM lakehouse.nyc_weather WHERE time <= now() ORDER BY time DESC LIMIT 1"

        try:
            count_df = self.execute_query(sql_count)
            zones_df = self.execute_query(sql_zones)
            top_df = self.execute_query(sql_top)
            weather_df = self.execute_query(sql_weather)
        except Exception as e:
            logger.error(f"Query error: {e}")
            return _clean_for_json(
                {
                    "total_trips": 0,
                    "total_revenue": 0.0,
                    "avg_fare": 0.0,
                    "active_zones": 0,
                    "top_zones": [],
                    "peak_hour": "N/A",
                    "temperature": 0.0,
                    "humidity": 0.0,
                    "precipitation": 0.0,
                }
            )

        if count_df.empty:
            return _clean_for_json(
                {
                    "total_trips": 0,
                    "total_revenue": 0.0,
                    "avg_fare": 0.0,
                    "active_zones": 0,
                    "top_zones": [],
                    "peak_hour": "N/A",
                    "temperature": 0.0,
                    "humidity": 0.0,
                    "precipitation": 0.0,
                }
            )

        row = count_df.iloc[0]
        active_zones = (
            int(zones_df.iloc[0]["active_zones"]) if not zones_df.empty else 0
        )

        totals = {
            "total_trips": int(row.get("cnt", 0) or 0),
            "total_revenue": float(row.get("rev", 0) or 0),
            "avg_fare": float(row.get("avg_f", 0) or 0),
            "active_zones": active_zones,
            "top_zones": top_df.replace([np.inf, -np.inf, np.nan], None).to_dict(
                orient="records"
            )
            if not top_df.empty
            else [],
            "peak_hour": "N/A",
        }

        if not weather_df.empty:
            weather_row = weather_df.iloc[0]
            totals["temperature"] = float(weather_row.get("temperature", 0) or 0)
            totals["humidity"] = float(weather_row.get("humidity", 0) or 0)
            totals["precipitation"] = float(weather_row.get("precipitation", 0) or 0)
        else:
            totals["temperature"] = 0.0
            totals["humidity"] = 0.0
            totals["precipitation"] = 0.0

        return _clean_for_json(totals)

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
        limit: int = None,
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

        # ClickHouse requires numeric LIMIT; use very large number if no limit specified
        limit_clause = f"LIMIT {limit}" if limit else "LIMIT 10000"

        sql = f"""
            SELECT 
                t.pulocationid as zone_id,
                IF(z.Zone = '' OR z.Zone IS NULL, 'Zone ' || toString(t.pulocationid), z.Zone) as zone_name,
                IF(z.Borough = '' OR z.Borough IS NULL, 'Unknown', z.Borough) as borough,
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
            {limit_clause}
        """
        df = self.execute_query(sql)
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
                    WHEN w.time IS NULL THEN 'No Weather Data'
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
                    WHEN w.time IS NULL THEN 'No Weather Data'
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
                FROM lakehouse.taxi_prod t
                {where_clause.replace("t.", "")}
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
                IF(z.Zone = '' OR z.Zone IS NULL, 'Zone ' || toString(pulocationid), z.Zone) as zone_name,
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
                IF(z.Zone = '' OR z.Zone IS NULL, 'Zone ' || toString(pulocationid), z.Zone) as zone_name,
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


class PostgresHistoricalService:
    """
    Query historical data directly from PostgreSQL source database.
    Uses connection pooling for better performance under load.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Singleton pattern to share connection pool across requests."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # Skip if already initialized (singleton)
        if hasattr(self, "connection_pool"):
            return

        import psycopg2
        from psycopg2 import pool

        self.conn_params = {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "weather"),
            "user": os.getenv("POSTGRES_USER", "admin"),
            "password": os.getenv("POSTGRES_PASSWORD", "admin"),
        }

        try:
            # Create threaded connection pool
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=getattr(settings, "PG_POOL_MIN", 2),
                maxconn=getattr(settings, "PG_POOL_MAX", 20),
                **self.conn_params,
            )
            self.cache = get_cache_service()
            logger.info(
                f"PostgreSQL connection pool initialized "
                f"(min={settings.PG_POOL_MIN}, max={settings.PG_POOL_MAX})"
            )
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            raise

    def _get_connection(self):
        """Get connection from pool."""
        try:
            return self.connection_pool.getconn()
        except Exception as e:
            logger.warning(f"Pool exhausted, creating temporary connection: {e}")
            # Fallback: create temporary connection if pool is exhausted
            import psycopg2

            return psycopg2.connect(**self.conn_params)

    def _return_connection(self, conn):
        """Return connection to pool."""
        try:
            self.connection_pool.putconn(conn)
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {e}")
            # If pool return fails, close connection
            try:
                conn.close()
            except:
                pass

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute query using connection pool."""
        conn = None
        try:
            conn = self._get_connection()
            df = pd.read_sql(sql, conn)
            return df
        except Exception as e:
            logger.error(f"PostgreSQL query error: {e}", extra={"sql": sql[:100]})
            raise
        finally:
            if conn:
                self._return_connection(conn)

    def get_pool_metrics(self) -> Dict[str, int]:
        """Get connection pool metrics for monitoring."""
        if not hasattr(self, "connection_pool"):
            return {}
        return {
            "pool_min": settings.PG_POOL_MIN,
            "pool_max": settings.PG_POOL_MAX,
        }

    def close_all(self):
        """Close all connections in pool (for shutdown)."""
        if hasattr(self, "connection_pool"):
            self.connection_pool.closeall()
            logger.info("PostgreSQL connection pool closed")

    def get_historical_stats(
        self, start_date: str = None, end_date: str = None
    ) -> Dict[str, Any]:
        where_clauses = []
        if start_date:
            where_clauses.append(f"tpep_pickup_datetime >= '{start_date}'::date")
        if end_date:
            where_clauses.append(
                f"tpep_pickup_datetime <= '{end_date}'::date + interval '1 day'"
            )
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql_count = f"""
            SELECT 
                COUNT(*) as cnt, 
                SUM(total_amount) as rev, 
                AVG(fare_amount) as avg_f 
            FROM yellow_taxi_trips {where_clause}
        """

        sql_zones = f"""
            SELECT COUNT(DISTINCT pulocationid) as active_zones 
            FROM yellow_taxi_trips {where_clause}
        """

        sql_top = f"""
            SELECT 
                COALESCE(z."Zone", 'Zone ' || t.pulocationid::text) as zone_name,
                COUNT(*) as trips,
                SUM(t.total_amount) as revenue
            FROM yellow_taxi_trips t 
            LEFT JOIN taxi_zones z ON t.pulocationid = z."LocationID"
            {where_clause} 
            GROUP BY t.pulocationid, z."Zone" 
            ORDER BY trips DESC 
            LIMIT 5
        """

        try:
            count_df = self.execute_query(sql_count)
            zones_df = self.execute_query(sql_zones)
            top_df = self.execute_query(sql_top)
        except Exception as e:
            logger.error(f"PostgreSQL historical stats error: {e}")
            return {
                "total_trips": 0,
                "total_revenue": 0,
                "avg_fare": 0,
                "active_zones": 0,
                "top_zones": [],
            }

        if count_df.empty:
            return {
                "total_trips": 0,
                "total_revenue": 0,
                "avg_fare": 0,
                "active_zones": 0,
                "top_zones": [],
            }

        row = count_df.iloc[0]
        active_zones = (
            int(zones_df.iloc[0]["active_zones"]) if not zones_df.empty else 0
        )

        return {
            "total_trips": int(row.get("cnt", 0) or 0),
            "total_revenue": float(row.get("rev", 0) or 0),
            "avg_fare": float(row.get("avg_f", 0) or 0),
            "active_zones": active_zones,
            "top_zones": top_df.to_dict(orient="records") if not top_df.empty else [],
        }

    def get_historical_time_series(
        self,
        metric: str,
        interval: str,
        start_date: str = None,
        end_date: str = None,
    ) -> Dict[str, Any]:
        metric_map = {
            "trip_count": "COUNT(*)",
            "revenue": "SUM(total_amount)",
            "avg_fare": "AVG(fare_amount)",
        }
        sql_metric = metric_map.get(metric, "COUNT(*)")

        interval_map = {
            "hour": "hour",
            "day": "day",
            "week": "week",
        }
        truncate_to = interval_map.get(interval, "hour")

        where_clauses = []
        if start_date:
            where_clauses.append(f"tpep_pickup_datetime >= '{start_date}'::date")
        if end_date:
            where_clauses.append(
                f"tpep_pickup_datetime <= '{end_date}'::date + interval '1 day'"
            )
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        limit = 8760
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
                to_char(date_trunc('{truncate_to}', tpep_pickup_datetime), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as timestamps,
                {sql_metric} as values
            FROM yellow_taxi_trips
            {where_clause}
            GROUP BY date_trunc('{truncate_to}', tpep_pickup_datetime)
            ORDER BY timestamps ASC
            LIMIT {limit}
        """
        df = self.execute_query(sql)

        return {
            "timestamps": df["timestamps"].tolist() if not df.empty else [],
            "values": [float(v) for v in df["values"].tolist()] if not df.empty else [],
            "metric_name": metric,
            "unit": "USD" if metric in ["revenue", "avg_fare"] else "Trips",
            "interval": interval,
        }

    def get_historical_zone_performance(
        self,
        start_date: str = None,
        end_date: str = None,
        limit: int = None,
        boroughs: list = None,
    ) -> List[Dict[str, Any]]:
        where_clauses = []
        if start_date:
            where_clauses.append(f"tpep_pickup_datetime >= '{start_date}'::date")
        if end_date:
            where_clauses.append(
                f"tpep_pickup_datetime <= '{end_date}'::date + interval '1 day'"
            )
        if boroughs:
            borough_list = "', '".join(boroughs)
            where_clauses.append(f"z.\"Borough\" IN ('{borough_list}')")

        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        limit_clause = f"LIMIT {limit}" if limit else ""

        sql = f"""
            SELECT 
                t.pulocationid as zone_id,
                COALESCE(z."Zone", 'Zone ' || t.pulocationid::text) as zone_name,
                COALESCE(z."Borough", 'Unknown') as borough,
                COUNT(*) as pickups,
                SUM(t.total_amount) as revenue,
                AVG(t.fare_amount) as avg_fare,
                AVG(t.trip_distance) as distance,
                COUNT(*) FILTER (WHERE t.dolocationid != t.pulocationid) as dropoffs
            FROM yellow_taxi_trips t
            LEFT JOIN taxi_zones z ON t.pulocationid = z."LocationID"
            {where_clause}
            GROUP BY t.pulocationid, z."Zone", z."Borough"
            ORDER BY pickups DESC
            {limit_clause}
        """
        df = self.execute_query(sql)
        return df.to_dict(orient="records") if not df.empty else []

    def get_weather_impact(
        self, start_date: str = None, end_date: str = None
    ) -> List[Dict[str, Any]]:
        where_clauses = []
        if start_date:
            where_clauses.append(f"t.tpep_pickup_datetime >= '{start_date}'::date")
        if end_date:
            where_clauses.append(
                f"t.tpep_pickup_datetime <= '{end_date}'::date + interval '1 day'"
            )
        if not where_clauses:
            where_clauses.append(
                "t.tpep_pickup_datetime >= NOW() - INTERVAL '720 hours'"
            )
        where_clause = "WHERE " + " AND ".join(where_clauses)

        sql = f"""
            SELECT 
                DATE(t.tpep_pickup_datetime) as date,
                TO_CHAR(t.tpep_pickup_datetime, 'HH24:00') as hour,
                CASE 
                    WHEN w.time IS NULL THEN 'No Weather Data'
                    WHEN w.precipitation > 5 THEN 'Rainy'
                    WHEN w.precipitation > 0 THEN 'Light Rain'
                    WHEN w.temperature < 0 THEN 'Cold'
                    WHEN w.temperature > 30 THEN 'Hot'
                    ELSE 'Clear'
                END as weather_condition,
                ROUND(AVG(w.temperature)::numeric, 1) as temperature,
                ROUND(AVG(w.humidity)::numeric, 1) as humidity,
                COUNT(*) as trips,
                ROUND(AVG(t.fare_amount)::numeric, 2) as avg_fare,
                ROUND(AVG(t.trip_distance)::numeric, 2) as avg_distance
            FROM yellow_taxi_trips t
            LEFT JOIN nyc_weather_hourly w 
                ON date_trunc('hour', t.tpep_pickup_datetime) = w.time
            {where_clause}
            GROUP BY DATE(t.tpep_pickup_datetime), 
                TO_CHAR(t.tpep_pickup_datetime, 'HH24:00'),
                CASE 
                    WHEN w.time IS NULL THEN 'No Weather Data'
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
        return df.to_dict(orient="records") if not df.empty else []

    def is_healthy(self) -> bool:
        try:
            conn = self._get_connection()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return False


def get_historical_service() -> PostgresHistoricalService:
    return PostgresHistoricalService()
