import logging
import os
from typing import Dict, Any, List
from trino.dbapi import connect
import pandas as pd

logger = logging.getLogger(__name__)


class TrinoService:
    """Query Iceberg Gold tables via Trino for historical analytics."""

    def __init__(self):
        self.host = os.getenv("TRINO_HOST", "trino")
        self.port = int(os.getenv("TRINO_PORT", "8080"))
        self.user = os.getenv("TRINO_USER", "admin")
        self.catalog = "iceberg"
        self.schema = "lakehouse"
        self._connection = None
        logger.info(f"TrinoService initialized (host={self.host}, port={self.port})")

    @property
    def connection(self):
        if self._connection is None:
            try:
                self._connection = connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    catalog=self.catalog,
                    schema=self.schema,
                )
                logger.info(f"Connected to Trino at {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Failed to connect to Trino: {e}")
                raise
        return self._connection

    def execute_query(self, sql: str) -> pd.DataFrame:
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame()
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Trino query error: {e}\nSQL: {sql}")
            raise

    def get_historical_stats(
        self, start_date: str = None, end_date: str = None
    ) -> Dict[str, Any]:
        where_clauses = []
        if start_date:
            where_clauses.append(f"metric_date >= DATE '{start_date}'")
        if end_date:
            where_clauses.append(f"metric_date <= DATE '{end_date}'")
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql_count = f"""
            SELECT
                SUM(total_trips) as cnt,
                SUM(total_revenue) as rev,
                AVG(avg_fare) as avg_f
            FROM lakehouse.gold_borough_summary
            {where_clause}
        """

        sql_zones = f"""
            SELECT COUNT(DISTINCT zone_id) as active_zones
            FROM lakehouse.gold_zone_performance
            {where_clause}
        """

        sql_top = f"""
            SELECT
                zone_name,
                total_trips as trips,
                total_revenue as revenue
            FROM lakehouse.gold_zone_performance
            {where_clause}
            ORDER BY total_trips DESC
            LIMIT 5
        """

        try:
            count_df = self.execute_query(sql_count)
            zones_df = self.execute_query(sql_zones)
            top_df = self.execute_query(sql_top)
        except Exception as e:
            logger.error(f"Failed to query historical stats: {e}")
            return {
                "total_trips": 0,
                "total_revenue": 0.0,
                "avg_fare": 0.0,
                "active_zones": 0,
                "top_zones": [],
            }

        if count_df.empty:
            return {
                "total_trips": 0,
                "total_revenue": 0.0,
                "avg_fare": 0.0,
                "active_zones": 0,
                "top_zones": [],
            }

        row = count_df.iloc[0]
        return {
            "total_trips": int(row["cnt"] or 0),
            "total_revenue": float(row["rev"] or 0),
            "avg_fare": float(row["avg_f"] or 0),
            "active_zones": int(zones_df.iloc[0]["active_zones"] or 0)
            if not zones_df.empty
            else 0,
            "top_zones": top_df.to_dict(orient="records") if not top_df.empty else [],
        }

    def get_historical_time_series(
        self, metric: str, interval: str, start_date: str = None, end_date: str = None
    ) -> Dict[str, Any]:
        metric_map = {
            "trip_count": "SUM(trip_count)",
            "revenue": "SUM(total_revenue)",
            "avg_fare": "AVG(avg_fare)",
        }
        sql_metric = metric_map.get(metric, "SUM(trip_count)")

        where_clauses = []
        if start_date:
            where_clauses.append(f"metric_date >= DATE '{start_date}'")
        if end_date:
            where_clauses.append(f"metric_date <= DATE '{end_date}'")
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT
                format_datetime(metric_date + interval 'hour' * metric_hour, 'yyyy-MM-dd''T''HH:mm:ss''Z''')as timestamps,
                {sql_metric} as values
            FROM lakehouse.gold_hourly_metrics
            {where_clause}
            GROUP BY metric_date, metric_hour
            ORDER BY timestamps ASC
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
        boroughs: List[str] = None,
        limit: int = None,
    ) -> List[Dict[str, Any]]:
        where_clauses = []
        if start_date:
            where_clauses.append(f"metric_date >= DATE '{start_date}'")
        if end_date:
            where_clauses.append(f"metric_date <= DATE '{end_date}''")
        if boroughs:
            borough_list = "', '".join(boroughs)
            where_clauses.append(f"borough IN ('{borough_list}')")

        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        limit_clause = f"LIMIT {limit}" if limit else ""

        sql = f"""
            SELECT
                zone_id,
                zone_name,
                borough,
                total_trips as pickups,
                total_revenue as revenue,
                avg_fare,
                avg_distance as distance
            FROM lakehouse.gold_zone_performance
            {where_clause}
            ORDER BY total_trips DESC
            {limit_clause}
        """

        df = self.execute_query(sql)
        return df.to_dict(orient="records") if not df.empty else []


def get_trino_service() -> TrinoService:
    try:
        return TrinoService()
    except Exception as e:
        logger.error(f"Failed to initialize Trino service: {e}")
        raise
