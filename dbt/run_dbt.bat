@echo off
cd cdc_medallion
call ..\\.venv\\Scripts\\activate
dbt run --profiles-dir .
