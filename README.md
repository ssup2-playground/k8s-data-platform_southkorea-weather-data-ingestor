# K8s Data Platform / South Korea Weather Data Ingestor

## Run Locally

```
export DATA_KEY=XXX
export MINIO_ENDPOINT=192.168.1.89:9000
export MINIO_ACCESS_KEY=root
export MINIO_SECRET_KEY=root123!
export MINIO_BUCKET=southkorea-weather
export MINIO_DIRECTORY=hourly-parquet
export REQUEST_DATE=20240125
export REQUEST_HOUR=00

python3 ingestor.py
```