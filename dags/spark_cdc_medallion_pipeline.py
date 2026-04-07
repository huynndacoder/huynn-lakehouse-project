"""
Airflow DAG for managing NYC Taxi Analytics CDC Spark Jobs

This DAG manages the medallion architecture CDC pipelines:
- Bronze: Raw CDC data from Kafka
- Silver: Cleaned and typed data

The Spark jobs run continuously as streaming queries.
"""

from airflow.decorators import dag, task
from datetime import datetime
import time
import requests

SPARK_MASTER = "http://spark-master:8080"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_JOBS_DIR = "/spark_jobs"

SPARK_CONF = [
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.lakehouse.type=hadoop",
    "--conf spark.sql.catalog.lakehouse.warehouse=s3a://datalake/warehouse",
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000",
    "--conf spark.hadoop.fs.s3a.access.key=admin",
    "--conf spark.hadoop.fs.s3a.secret.key=admin12345",
    "--conf spark.hadoop.fs.s3a.path.style.access=true",
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "--conf spark.cores.max=4",
    "--conf spark.executor.cores=2",
    "--conf spark.default.parallelism=8",
]


def wait_for_service(host: str, port: int, timeout: int = 300) -> bool:
    """Wait for a service to become available."""
    import socket

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return True
        except Exception:
            pass
        time.sleep(5)
    return False


def get_spark_apps():
    """Get list of running Spark applications."""
    try:
        response = requests.get(f"{SPARK_MASTER}/api/v1/applications", timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return []


def submit_spark_job(job_name: str, job_file: str, app_name: str) -> bool:
    """Submit a Spark job using docker exec."""
    import subprocess

    conf_args = " ".join(SPARK_CONF)
    cmd = [
        "docker",
        "exec",
        "spark-master",
        "bash",
        "-c",
        f"{SPARK_SUBMIT} --master spark://spark-master:7077 "
        f"--name {app_name} {conf_args} {SPARK_JOBS_DIR}/{job_file}",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return True
        else:
            print(f"Job submission returned code {result.returncode}")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
    except Exception as e:
        print(f"Failed to submit job: {e}")

    return False


def stop_spark_job(app_id: str) -> bool:
    """Stop a Spark application."""
    import subprocess

    cmd = [
        "docker",
        "exec",
        "spark-master",
        "bash",
        "-c",
        f"{SPARK_SUBMIT} --master spark://spark-master:7077 --kill {app_id}",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except Exception:
        return False


@dag(
    dag_id="spark_cdc_medallion_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="Manages medallion CDC Spark jobs for NYC Taxi Analytics",
    max_active_runs=1,
)
def spark_cdc_pipeline():

    @task
    def check_dependencies():
        """Wait for all required services to be healthy."""
        print("Checking service dependencies...")

        services = [
            ("kafka", "kafka", 9092),
            ("minio", "minio", 9000),
            ("spark-master", "spark-master", 7077),
        ]

        for name, host, port in services:
            print(f"  Checking {name} ({host}:{port})...")
            if not wait_for_service(host, port):
                raise Exception(f"Service {name} not available")
            print(f"  [OK] {name} is ready")

        return True

    @task
    def submit_taxi_cdc():
        """Submit the Taxi Medallion CDC job."""
        print("Submitting TaxiMedallionCDC job...")

        running_apps = get_spark_apps()
        for app in running_apps:
            if app.get("name") == "TaxiMedallionCDC":
                print("  TaxiMedallionCDC already running")
                return {"status": "already_running", "app_id": app["id"]}

        if submit_spark_job("TaxiMedallionCDC", "taxi_cdc.py", "TaxiMedallionCDC"):
            print("  TaxiMedallionCDC submitted successfully")
            return {"status": "submitted"}
        else:
            raise Exception("Failed to submit TaxiMedallionCDC")

    @task
    def submit_weather_cdc():
        """Submit the Weather Medallion CDC job."""
        print("Submitting WeatherMedallionCDC job...")

        running_apps = get_spark_apps()
        for app in running_apps:
            if app.get("name") == "WeatherMedallionCDC":
                print("  WeatherMedallionCDC already running")
                return {"status": "already_running", "app_id": app["id"]}

        if submit_spark_job(
            "WeatherMedallionCDC", "weather_cdc.py", "WeatherMedallionCDC"
        ):
            print("  WeatherMedallionCDC submitted successfully")
            return {"status": "submitted"}
        else:
            raise Exception("Failed to submit WeatherMedallionCDC")

    @task
    def verify_jobs_running():
        """Verify that Spark jobs are running."""
        print("Verifying Spark jobs...")

        time.sleep(10)
        running_apps = get_spark_apps()

        taxi_running = any(
            app.get("name") == "TaxiMedallionCDC" for app in running_apps
        )
        weather_running = any(
            app.get("name") == "WeatherMedallionCDC" for app in running_apps
        )

        print(f"  TaxiMedallionCDC: {'RUNNING' if taxi_running else 'NOT RUNNING'}")
        print(
            f"  WeatherMedallionCDC: {'RUNNING' if weather_running else 'NOT RUNNING'}"
        )

        if not taxi_running:
            print("  WARNING: TaxiMedallionCDC is not running!")
        if not weather_running:
            print("  WARNING: WeatherMedallionCDC is not running!")

        return {
            "taxi_running": taxi_running,
            "weather_running": weather_running,
            "apps": running_apps,
        }

    @task
    def restart_failed_jobs():
        """Restart any failed Spark jobs."""
        print("Checking for failed jobs to restart...")

        running_apps = get_spark_apps()

        for app in running_apps:
            if app.get("name") in ["TaxiMedallionCDC", "WeatherMedallionCDC"]:
                state = (
                    app.get("attempts", [{}])[0]
                    .get("appState", {})
                    .get("state", "UNKNOWN")
                )
                if state == "FAILED":
                    print(f"  Job {app['name']} failed, will restart...")
                    stop_spark_job(app["id"])
                    time.sleep(5)

        return {"action": "checked_failures"}

    deps = check_dependencies()
    taxi_result = submit_taxi_cdc()
    weather_result = submit_weather_cdc()

    deps >> [taxi_result, weather_result]

    verify_result = verify_jobs_running()
    [taxi_result, weather_result] >> verify_result

    restart_failed_jobs() >> verify_result


dag_instance = spark_cdc_pipeline()
