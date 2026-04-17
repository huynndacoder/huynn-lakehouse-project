import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, Security, status, Query, Response
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from config import settings
from models import APIResponse
from database import (
    DatabaseService,
    get_db_service,
    PostgresHistoricalService,
    get_historical_service,
)
from doris_service import DorisService, get_doris_service


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


# Lifespan Context
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🚀 Starting up {settings.APP_NAME}...")
    # will trigger the DB connection pool initialization here later
    yield
    logger.info(f"🛑 Shutting down {settings.APP_NAME}...")


# App Init
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
    expose_headers=["X-Data-Source"],
)

# Endpoints


@app.get("/health", response_model=APIResponse, tags=["System"])
async def health_check(db: DatabaseService = Depends(get_db_service)):
    """Health check endpoint (No Auth Required)"""
    is_connected = db.is_healthy()
    return APIResponse(
        success=True,
        message="API is operational",
        data={"database_connected": is_connected, "environment": settings.ENVIRONMENT},
    )


@app.get("/health/pool", tags=["System"])
async def pool_metrics(
    db: DatabaseService = Depends(get_db_service),
    historical: PostgresHistoricalService = Depends(get_historical_service),
):
    """
    Get connection pool metrics for monitoring.

    Returns:
        - ClickHouse pool: size, checked_in, checked_out, overflow
        - PostgreSQL pool: min, max connections
        - Redis cache: enabled status
    """
    from cache import get_cache_service

    ch_metrics = db.get_pool_metrics()
    pg_metrics = historical.get_pool_metrics()

    cache = get_cache_service()
    cache_status = {
        "enabled": cache.enabled if cache else False,
        "connected": cache.health_check() if cache else False,
    }

    return {
        "clickhouse_pool": ch_metrics,
        "postgresql_pool": pg_metrics,
        "redis_cache": cache_status,
    }


@app.get("/api/v1/dashboard/stats", response_model=APIResponse, tags=["Dashboard"])
async def get_dashboard_stats(
    response: Response,
    mode: str = Query("realtime", regex="^(realtime|historical)$"),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    hours_back: Optional[int] = Query(
        None, description="Filter last N hours (overrides dates)"
    ),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
    historical: PostgresHistoricalService = Depends(get_historical_service),
):
    try:
        if mode == "historical":
            try:
                doris = get_doris_service()
                data = doris.get_historical_stats(
                    start_date=start_date, end_date=end_date
                )
                logger.info("Historical stats from Doris/Iceberg")
                
                response.headers["X-Data-Source"] = "Doris (Iceberg)"
                
                return APIResponse(
                    success=True,
                    message="Stats retrieved from Iceberg Gold via Doris",
                    data=data,
                )
                
            except Exception as doris_error:
                logger.warning(
                    f"Doris query failed, falling back to PostgreSQL: {doris_error}"
                )
                data = historical.get_historical_stats(
                    start_date=start_date, end_date=end_date
                )
                
                response.headers["X-Data-Source"] = "PostgreSQL (Fallback)"
                
                return APIResponse(
                    success=True,
                    message="Stats retrieved from PostgreSQL (Doris fallback)",
                    data=data,
                )
        else:
            data = db.get_dashboard_stats(
                start_date=start_date, end_date=end_date, hours_back=hours_back
            )
            
            response.headers["X-Data-Source"] = "ClickHouse (Real-time)"
            
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
    response: Response,
    mode: str = Query("realtime", regex="^(realtime|historical)$"),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    hours_back: Optional[int] = Query(None, description="Hours of data (for real-time)"),
    limit: Optional[int] = Query(None, ge=1, le=500, description="Max zones to return"),
    boroughs: Optional[str] = Query(None, description="Comma-separated borough names"),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
    historical: PostgresHistoricalService = Depends(get_historical_service),
):
    try:
        borough_list = boroughs.split(",") if boroughs else None
        if mode == "historical":
            try:
                doris = get_doris_service()
                data = doris.get_historical_zone_performance(
                    start_date=start_date, end_date=end_date, boroughs=borough_list, limit=limit
                )
                logger.info("✅ Zone analytics served by Doris (Iceberg)")
                msg = "Zone analytics retrieved from Doris"
                response.headers["X-Data-Source"] = "Doris (Iceberg)"
            except Exception as e:
                logger.warning(f"⚠️ Doris zone query failed, falling back to PostgreSQL: {e}")
                data = historical.get_historical_zone_performance(
                    start_date, end_date, limit, borough_list
                )
                msg = "Zone analytics retrieved from PostgreSQL (Fallback)"
                response.headers["X-Data-Source"] = "PostgreSQL (Fallback)"
        else:
            data = db.get_zone_performance(
                start_date, end_date, limit, hours_back, borough_list
            )
            msg = "Zone analytics retrieved from ClickHouse"
            response.headers["X-Data-Source"] = "ClickHouse (Real-time)"
            
        return APIResponse(success=True, message=msg, data=data)
    except Exception as e:
        logger.error(f"Zone analytics error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/api/v1/analytics/weather-impact", response_model=APIResponse, tags=["Analytics"]
)
async def get_weather_impact(
    mode: str = Query("realtime", regex="^(realtime|historical)$"),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    hours_back: int = Query(720, ge=1, le=8760, description="Hours of data to analyze"),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
    historical: PostgresHistoricalService = Depends(get_historical_service),
):
    try:
        if mode == "historical":
            data = historical.get_weather_impact(start_date, end_date)
        else:
            data = db.get_weather_impact(
                start_date=start_date, end_date=end_date, hours_back=hours_back
            )
        return APIResponse(success=True, message="Weather impact retrieved", data=data)
    except Exception as e:
        logger.error(f"Weather impact error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/v1/analytics/time-series", response_model=APIResponse, tags=["Analytics"])
async def get_time_series(
    response: Response,
    mode: str = Query("realtime", regex="^(realtime|historical)$"),
    metric: str = Query("trip_count", regex="^(trip_count|revenue|avg_fare)$"),
    interval: str = Query("hour", regex="^(hour|day|week)$"),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    hours_back: Optional[int] = Query(None, description="Filter last N hours (overrides dates)"),
    api_key: str = Depends(verify_api_key),
    db: DatabaseService = Depends(get_db_service),
    historical: PostgresHistoricalService = Depends(get_historical_service),
):
    try:
        if mode == "historical":
            try:
                doris = get_doris_service()
                data = doris.get_historical_time_series(
                    metric=metric, interval=interval, start_date=start_date, end_date=end_date
                )
                logger.info("✅ Time series served by Doris (Iceberg)")
                msg = f"Time series for {metric} from Doris"
                response.headers["X-Data-Source"] = "Doris (Iceberg)"
            except Exception as e:
                logger.warning(f"⚠️ Doris time-series query failed, falling back to PostgreSQL: {e}")
                data = historical.get_historical_time_series(
                    metric, interval, start_date=start_date, end_date=end_date
                )
                msg = f"Time series for {metric} from PostgreSQL (Fallback)"
                response.headers["X-Data-Source"] = "PostgreSQL (Fallback)"
        else:
            data = db.get_time_series(
                metric, interval, start_date=start_date, end_date=end_date, hours_back=hours_back
            )
            msg = f"Time series for {metric} from ClickHouse"
            response.headers["X-Data-Source"] = "ClickHouse (Real-time)"
            
        return APIResponse(success=True, message=msg, data=data)
    except Exception as e:
        logger.error(f"Time series error: {e}")
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
