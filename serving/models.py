from datetime import datetime, timezone
from typing import List, Optional, Generic, TypeVar
from pydantic import BaseModel, Field

# --- Type Variables for Generics ---
T = TypeVar('T')

# ==========================================
# 1. API Response Wrapper Models
# ==========================================

class APIResponse(BaseModel, Generic[T]):
    """Standard wrapper for all API responses."""
    success: bool = True
    message: str = ""
    data: Optional[T] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class PaginatedResponse(APIResponse[List[T]], Generic[T]):
    """Standard wrapper for paginated list responses."""
    total_count: int
    page: int
    page_size: int
    total_pages: int

# ==========================================
# 2. Trip Models
# ==========================================

class TripBase(BaseModel):
    """Base model containing core trip data."""
    pickup_datetime: datetime
    dropoff_datetime: datetime
    pickup_location_id: int
    dropoff_location_id: int
    passenger_count: Optional[int] = None
    trip_distance: Optional[float] = None
    fare_amount: float
    tip_amount: float
    total_amount: float

class TripResponse(TripBase):
    """Enriched trip data returned to the client."""
    trip_id: Optional[str] = None
    vendor_id: Optional[int] = None
    payment_type: Optional[int] = None
    pickup_zone: Optional[str] = None
    dropoff_zone: Optional[str] = None

# ==========================================
# 3. Analytics Models
# ==========================================

class ZoneMetrics(BaseModel):
    """Aggregated performance metrics for a specific taxi zone."""
    zone_id: int
    zone_name: str
    borough: str
    pickups: int
    dropoffs: int
    avg_fare: float
    revenue: float
    distance: float
    peak_hour_factor: Optional[float] = None

class WeatherImpact(BaseModel):
    """Correlation analysis between weather and trip volume."""
    date: datetime
    weather_condition: str
    temperature: float
    humidity: float
    trips: int
    avg_fare: float
    impact_score: float

class DemandPrediction(BaseModel):
    """Machine learning prediction output for zone demand."""
    location_id: int
    zone_name: str
    prediction_hour: datetime
    predicted_demand: str
    confidence: float
    weather_impact: float
    historical_avg: float

class RealTimeActivity(BaseModel):
    """Live monitoring data for zones."""
    zone_id: int
    zone_name: str
    timestamp: datetime
    activity_score: float
    pickup_count: int
    revenue: float
    avg_wait_time: Optional[float] = None

# ==========================================
# 4. Dashboard Models
# ==========================================

class TopZone(BaseModel):
    """Strictly typed model replacing the Dict[str, Any] anti-pattern."""
    zone_name: str
    trips: int
    revenue: float

class DashboardStats(BaseModel):
    """High-level summary metrics for the main dashboard."""
    total_trips_today: int
    total_revenue_today: float
    avg_fare_today: float
    top_zones: List[TopZone] = Field(default_factory=list)

class TimeSeriesData(BaseModel):
    """Data shape designed specifically for rendering charts."""
    timestamps: List[datetime]
    values: List[float]
    metric_name: str
    unit: str

# ==========================================
# 5. Filter & Query Models
# ==========================================

class DateRangeFilter(BaseModel):
    """Filter for time boundaries."""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

class ZoneFilter(BaseModel):
    """Filter for location boundaries."""
    zone_ids: Optional[List[int]] = None
    borough: Optional[str] = None

class AnalyticsFilter(BaseModel):
    """Combined filter using composition pattern."""
    date_range: Optional[DateRangeFilter] = None
    zones: Optional[ZoneFilter] = None
    weather_conditions: Optional[List[str]] = None
    min_fare: Optional[float] = None
    max_fare: Optional[float] = None