## URLs
    kafka ui: http://localhost:8080
    minio: http://localhost:9001

## Docker build
    docker compose up -d
    docker compose ps

## Kafka check
    curl http://localhost:8083/
    curl http://localhost:8083/connectors

## Oracle log
    docker logs -f oracle
    docker logs oracle 2>&1 | grep -E "DATABASE IS READY|Table created|Supplemental"
    docker exec -it oracle sqlplus app/apppassword@//localhost:1521/XEPDB1

