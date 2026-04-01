import logging
import requests
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import date, datetime

logger = logging.getLogger(__name__)

class LakehouseClient:
    """
    Python client for interacting with the NYC Taxi Real-Time Analytics API.
    Handles authentication, connection pooling, and DataFrame conversions.
    """
    
    def __init__(self, base_url: str = "http://localhost:8000/api/v1", api_key: str = "demo-api-key-2024"):
        self.base_url = base_url.rstrip("/")
        # Extract the root host (e.g., http://localhost:8000) for root endpoints like /health
        self.host_url = "/".join(self.base_url.split("/")[:3]) 
        
        # Setup connection pooling and session-level auth
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": api_key,
            "Accept": "application/json"
        })
        logger.info(f"LakehouseClient initialized. Base URL: {self.base_url}")

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None, is_root: bool = False) -> Any:
        """Helper method to execute GET requests, handle errors, and parse the APIResponse wrapper."""
        url = f"{self.host_url}/{endpoint.lstrip('/')}" if is_root else f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            payload = response.json()
            if not payload.get("success"):
                raise Exception(f"API Error: {payload.get('message')}")
                
            return payload.get("data")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP Request failed for {url}: {e}")
            raise

    # ==========================================
    # API Methods
    # ==========================================

    def health_check(self) -> Dict[str, Any]:
        """Check API and Database health. Uses the root host URL."""
        return self._make_request("health", is_root=True)

    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get high-level statistics for the dashboard."""
        return self._make_request("dashboard/stats")

    def get_recent_trips(self, limit: int = 50, hours_back: int = 24) -> pd.DataFrame:
        """Fetch recent trips and return as a pandas DataFrame."""
        params = {"limit": limit, "hours_back": hours_back}
        data = self._make_request("trips/recent", params=params)
        df = pd.DataFrame(data)
        if not df.empty:
            df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
            df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
        return df

    def get_zone_metrics(self, start_date: Optional[date] = None, end_date: Optional[date] = None, limit: int = 10) -> pd.DataFrame:
        """Get performance metrics grouped by taxi zones."""
        params = {"limit": limit}
        if start_date: params["start_date"] = start_date.isoformat()
        if end_date: params["end_date"] = end_date.isoformat()
            
        data = self._make_request("analytics/zones", params=params)
        return pd.DataFrame(data)

    def get_weather_impact(self) -> pd.DataFrame:
        """Analyze weather impact on trips."""
        data = self._make_request("analytics/weather-impact")
        df = pd.DataFrame(data)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df

    def get_time_series(self, metric: str = "trip_count", interval: str = "hour") -> Dict[str, Any]:
        """Get time-series data for rendering charts."""
        params = {"metric": metric, "interval": interval}
        return self._make_request("analytics/time-series", params=params)

    def get_demand_predictions(self) -> pd.DataFrame:
        """Get ML predictions for zone demand."""
        data = self._make_request("predictions/demand")
        df = pd.DataFrame(data)
        if not df.empty:
            df['prediction_hour'] = pd.to_datetime(df['prediction_hour'])
        return df

    def get_real_time_activity(self) -> pd.DataFrame:
        """Get real-time zone activity for map rendering."""
        data = self._make_request("realtime/activity")
        df = pd.DataFrame(data)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df

    def export_trips(self, export_format: str = "json") -> Any:
        """Export trip data in JSON or CSV format."""
        params = {"format": export_format}
        if export_format == "csv":
            response = self.session.get(f"{self.base_url}/export/trips", params=params)
            response.raise_for_status()
            return response.text # Return raw CSV string
        else:
            return self._make_request("export/trips", params=params)

# ==========================================
# Utility Functions
# ==========================================

def example_usage():
    """Demonstrates all client methods."""
    logging.basicConfig(level=logging.INFO)
    client = LakehouseClient()
    
    print("--- 1. Health Check ---")
    print(client.health_check())
    
    print("\n--- 2. Dashboard Stats ---")
    print(client.get_dashboard_stats())
    
    print("\n--- 3. Recent Trips DataFrame ---")
    df_trips = client.get_recent_trips(limit=5)
    print(df_trips.head())

def create_sample_analysis():
    """Runnable analysis script with formatted output."""
    client = LakehouseClient()
    print("🚕 NYC Taxi Lakehouse Analysis Report 🚕")
    print("========================================")
    
    stats = client.get_dashboard_stats()
    print(f"Total Revenue Today: ${stats.get('total_revenue_today', 0):,.2f}")
    print(f"Total Trips Today: {stats.get('total_trips_today', 0):,}")
    
    print("\nTop 3 Zones by Trips:")
    df_zones = client.get_zone_metrics(limit=3)
    if not df_zones.empty:
        for _, row in df_zones.iterrows():
            print(f"- {row['zone_name']}: {row['pickups']:,} trips (${row['revenue']:,.2f})")
    
    print("\nWeather Impact Summary:")
    df_weather = client.get_weather_impact()
    if not df_weather.empty:
        print(df_weather[['weather_condition', 'trips', 'avg_fare']].to_string(index=False))

if __name__ == "__main__":
    # Run the sample analysis if executed directly
    create_sample_analysis()