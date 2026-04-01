import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, Security, status, Query
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# --- LEGO BRICK IMPORTS (To be built next) ---
from config import settings
from models import APIResponse
from database import DatabaseService, get_db_service

# --- Setup & Security ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

api_key_header = APIKeyHeader(name=settings.API_KEY_NAME, auto_error=False)


async def verify_api_key(api_key: str = Security(api_key_header)):
    """Dependency to verify the X-API-Key header."""
    if api_key != settings.API_KEY:
        logger.warning(f"Unauthorized access attempt with key: {api_key}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate API credentials",
        )
    return api_key


# --- Lifespan Context ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🚀 Starting up {settings.APP_NAME}...")
    # We will trigger the DB connection pool initialization here later
    yield
    logger.info(f"🛑 Shutting down {settings.APP_NAME}...")


# --- App Initialization ---
app = FastAPI(
    title=settings.APP_NAME,
    description="REST API serving real-time NYC Taxi data from ClickHouse",
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoints ---


@app.get("/health", response_model=APIResponse, tags=["System"])
async def health_check(db: DatabaseService = Depends(get_db_service)):
    """Health check endpoint (No Auth Required)"""
    is_connected = db.is_healthy()
    return APIResponse(
        success=True,
        message="API is operational",
        data={"database_connected": is_connected, "environment": settings.ENVIRONMENT},
    )


@app.get("/api/v1/dashboard/stats", response_model=APIResponse, tags=["Dashboard"])
async def get_dashboard_stats(
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    hours_back: Optional[int] = Query(
        None, description="Filter last N hours (overrides dates)"
    ),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
):
    try:
        data = db.get_dashboard_stats(
            start_date=start_date, end_date=end_date, hours_back=hours_back
        )
        return APIResponse(success=True, message="Stats retrieved", data=data)
    except Exception as e:
        logger.error(f"Dashboard stats error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/v1/trips/recent", response_model=APIResponse, tags=["Trips"])
async def get_recent_trips(
    limit: int = Query(50, ge=1, le=1000),
    hours_back: int = Query(24, ge=1, le=720),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
):
    try:
        data = db.get_recent_trips(limit=limit, hours_back=hours_back)
        return APIResponse(success=True, message=f"Fetched {limit} trips", data=data)
    except Exception as e:
        logger.error(f"Recent trips error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/v1/analytics/zones", response_model=APIResponse, tags=["Analytics"])
async def get_zone_analytics(
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    hours_back: Optional[int] = Query(
        None, description="Hours of data (for real-time)"
    ),
    limit: int = Query(10, ge=1, le=50),
    boroughs: Optional[str] = Query(None, description="Comma-separated borough names"),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
):
    try:
        borough_list = boroughs.split(",") if boroughs else None
        data = db.get_zone_performance(
            start_date, end_date, limit, hours_back, borough_list
        )
        return APIResponse(success=True, message="Zone analytics retrieved", data=data)
    except Exception as e:
        logger.error(f"Zone analytics error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/api/v1/analytics/weather-impact", response_model=APIResponse, tags=["Analytics"]
)
async def get_weather_impact(
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    hours_back: int = Query(720, ge=1, le=8760, description="Hours of data to analyze"),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
):
    try:
        data = db.get_weather_impact(
            start_date=start_date, end_date=end_date, hours_back=hours_back
        )
        return APIResponse(success=True, message="Weather impact retrieved", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/api/v1/analytics/time-series", response_model=APIResponse, tags=["Analytics"]
)
async def get_time_series(
    metric: str = Query("trip_count", regex="^(trip_count|revenue|avg_fare)$"),
    interval: str = Query("hour", regex="^(hour|day|week)$"),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    hours_back: Optional[int] = Query(
        None, description="Filter last N hours (overrides dates)"
    ),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
):
    try:
        data = db.get_time_series(
            metric,
            interval,
            start_date=start_date,
            end_date=end_date,
            hours_back=hours_back,
        )
        return APIResponse(success=True, message=f"Time series for {metric}", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/v1/predictions/demand", response_model=APIResponse, tags=["Predictions"])
async def get_demand_predictions(
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
):
    try:
        data = db.get_demand_predictions()
        return APIResponse(
            success=True, message="Demand predictions retrieved", data=data
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/v1/realtime/activity", response_model=APIResponse, tags=["Real-Time"])
async def get_realtime_activity(
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
):
    try:
        data = db.get_realtime_activity()
        return APIResponse(
            success=True, message="Real-time activity retrieved", data=data
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/v1/export/trips", tags=["Export"])
async def export_trips(
    format: str = Query("json", regex="^(json|csv)$"),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
):
    try:
        if format == "csv":
            stream = db.export_trips_csv()
            return StreamingResponse(
                stream,
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=trips.csv"},
            )

        data = db.get_recent_trips(limit=1000)  # JSON export limit
        return APIResponse(success=True, message="Export generated", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
