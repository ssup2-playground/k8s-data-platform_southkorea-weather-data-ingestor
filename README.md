# K8s Data Platform / South Korea Weather Data Ingestor

## Run Locally

```
export DATA_KEY=6Z0Atvi5Nos0uS8cgukb1JJto9dOhHNRjcq0yWsZcu3ZWc1HzqgcREbpeSCaV7Sm5BaLOb1+fBXx/K/HcndM7A==
export MINIO_ENDPOINT=192.168.1.89:9000
export MINIO_ACCESS_KEY=root
export MINIO_SECRET_KEY=root123!
export MINIO_BUCKET=southkorea-weather
export MINIO_DIRECTORY=hourly-parquet
export REQUEST_DATE=20240125
export REQUEST_HOUR=00

python3 ingestor.py
```