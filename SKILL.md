---
name: Alchemurgist
category: transformation
version: 1.0.0
purpose: Data transformation and ETL pipelines with Apache Airflow
tags: [etl, transformation, data, airflow, pipeline, orchestration]
author: OpenClaw
description: Master Apache Airflow for building robust ETL pipelines, data workflows, and transformation logic with the Alchemurgist skill
requires:
  - airflow
  - python3.8+
  - postgres/mysql (optional for metadata)
---

# Alchemurgist - Data Transformation & ETL with Apache Airflow

## Overview

Alchemurgist provides expert-level capabilities for designing, building, and maintaining Apache Airflow DAGs for data transformation and ETL pipelines. This skill transforms raw data into refined insights through orchestrated workflows.

## Installation & Setup

### Prerequisites

```bash
pip install apache-airflow==2.8.0
pip install apache-airflow-providers-postgres
pip install apache-airflow-providers-snowflake
pip install pandas pyarrow
```

### Initialize Airflow

```bash
# Initialize metadata database
airflow db init

# Create admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Start webserver (port 8080)
airflow webserver -p 8080

# Start scheduler (separate terminal)
airflow scheduler
```

## Core Concepts

### DAG Structure

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'alchemurgist',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_transformation_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['etl', 'transformation'],
) as dag:
    
    extract = BashOperator(
        task_id='extract_data',
        bash_command='python /scripts/extract.py',
    )
    
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_logic,
        op_kwargs={'source': 'raw', 'target': 'staging'},
    )
    
    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_snowflake,
    )
    
    extract >> transform >> load
```

## Real Use Cases

### Use Case 1: Daily Sales Data Pipeline

```python
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'data_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

def extract_sales(**context):
    """Extract sales data from API and store in staging."""
    import requests
    
    execution_date = context['ds']
    url = f"https://api.sales.example.com/daily?date={execution_date}"
    
    response = requests.get(url, timeout=30)
    data = response.json()
    
    df = pd.DataFrame(data['sales'])
    df['extracted_at'] = datetime.now()
    
    df.to_csv(f'/tmp/sales_{execution_date}.csv', index=False)
    return f'/tmp/sales_{execution_date}.csv'

def transform_sales(**context):
    """Clean, enrich, and aggregate sales data."""
    filepath = context['ti'].xcom_pull(task_ids='extract_sales')
    df = pd.read_csv(filepath)
    
    # Clean data
    df = df.dropna(subset=['product_id', 'amount'])
    df['amount'] = df['amount'].astype(float)
    
    # Add derived fields
    df['tax'] = df['amount'] * 0.21
    df['total'] = df['amount'] + df['tax']
    df['region'] = df['product_id'].map(lambda x: get_region(x))
    
    # Aggregate
    daily_summary = df.groupby(['region', 'date']).agg({
        'amount': 'sum',
        'tax': 'sum',
        'total': 'sum',
        'product_id': 'count'
    }).reset_index()
    
    daily_summary.to_csv(f'/tmp/sales_summary_{context["ds"]}.csv', index=False)
    return f'/tmp/sales_summary_{context["ds"]}.csv'

def check_data_quality(**context):
    """Validate data meets quality thresholds."""
    filepath = context['ti'].xcom_pull(task_ids='transform_sales')
    df = pd.read_csv(filepath)
    
    if len(df) == 0:
        raise ValueError("No data to process")
    
    if df['total'].sum() < 100:
        raise ValueError(f"Revenue too low: {df['total'].sum()}")
    
    return "Quality checks passed"

with DAG(
    'daily_sales_etl',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 3 * * *',
    catchup=False,
) as dag:
    
    start = DummyOperator(task_id='start')
    
    extract = PythonOperator(
        task_id='extract_sales',
        python_callable=extract_sales,
    )
    
    transform = PythonOperator(
        task_id='transform_sales',
        python_callable=transform_sales,
    )
    
    quality_check = BranchPythonOperator(
        task_id='check_data_quality',
        python_callable=lambda **ctx: 'load_to_warehouse' if check_data_quality(**ctx) else 'notify_failure',
    )
    
    load = PostgresOperator(
        task_id='load_to_warehouse',
        postgres_conn_id='warehouse_conn',
        sql="""
            INSERT INTO daily_sales (region, date, amount, tax, total, product_count)
            VALUES {{ ti.xcom_pull(task_ids='transform_sales')|from_csv }}
        """,
    )
    
    notify_failure = DummyOperator(task_id='notify_failure')
    
    start >> extract >> transform >> quality_check >> [load, notify_failure]
```

### Use Case 2: Customer Data Deduplication Pipeline

```python
def deduplicate_customers(**context):
    """Identify and merge duplicate customer records."""
    from sqlalchemy import create_engine
    
    engine = create_engine(context['conn_string'])
    
    # Load raw customer data
    df = pd.read_sql("SELECT * FROM raw_customers", engine)
    
    # Create matching keys
    df['match_key'] = (
        df['email'].str.lower().str.strip() + '_' +
        df['last_name'].str.lower().str.strip()
    )
    
    # Find duplicates
    duplicates = df[df.duplicated(subset=['match_key'], keep=False)]
    
    # Group and consolidate
    merged = df.groupby('match_key').agg({
        'customer_id': 'first',
        'email': 'first',
        'first_name': 'first',
        'last_name': 'first',
        'phone': 'first',
        'created_at': 'min',
        'updated_at': 'max',
        'total_orders': 'sum',
    }).reset_index()
    
    merged.to_sql('deduplicated_customers', engine, if_exists='replace', index=False)
    
    return {'duplicates_found': len(duplicates), 'unique_records': len(merged)}
```

### Use Case 3: Real-time Sensor Data Processing

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

class SensorDataAvailableSensor(BaseSensorOperator):
    """Check if sensor data files are available."""
    
    @apply_defaults
    def __init__(self, filepath_pattern, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filepath_pattern = filepath_pattern
    
    def poke(self, context):
        import glob
        import os
        
        files = glob.glob(self.filepath_pattern)
        
        if not files:
            return False
        
        # Check file is not being written
        latest = max(files, key=os.path.getmtime)
        size_now = os.path.getsize(latest)
        
        import time
        time.sleep(2)
        size_later = os.path.getsize(latest)
        
        return size_now == size_later

with DAG('sensor_data_pipeline', schedule_interval='*/15 * * * *') as dag:
    wait_for_data = SensorDataAvailableSensor(
        task_id='wait_for_sensor_data',
        filepath_pattern='/data/sensors/*.json',
        poke_interval=60,
        timeout=3600,
    )
    
    process = PythonOperator(
        task_id='process_sensor_data',
        python_callable=process_sensor_readings,
    )
```

## Specific Commands

### DAG Management

```bash
# List all DAGs
airflow dags list

# Show DAG details
airflow dags report

# Trigger DAG manually
airflow dags trigger -e 2024-01-15 daily_sales_etl

# Pause/Unpause DAG
airflow dags pause daily_sales_etl
airflow dags unpause daily_sales_etl

# Backfill DAG (run for historical dates)
airflow dags backfill -s 2024-01-01 -e 2024-01-31 daily_sales_etl

# Clear task instance (retry failed)
airflow tasks clear daily_sales_etl transform_sales -d 2024-01-15 -s failed

# Show task logs
airflow logs daily_sales_etl transform_sales 2024-01-15
```

### Task Operations

```bash
# List task instances for DAG
airflow tasks list daily_sales_etl

# Show task state
airflow tasks state daily_sales_etl transform_sales 2024-01-15

# Run task locally (testing)
airflow tasks test daily_sales_etl transform_sales 2024-01-15

# Kill running task
airflow tasks kill
```

### Web UI Endpoints

```
http://localhost:8080 - Airflow Web UI
http://localhost:8080/graph?dag_id=daily_sales_etl - Graph view
http://localhost:8080/gantt?dag_id=daily_sales_etl - Gantt chart
http://localhost:8080/code?dag_id=daily_sales_etl - View DAG code
```

## Troubleshooting

### Issue: Tasks Stuck in "queued" State

**Cause**: Scheduler not running or worker capacity issues.

**Solution**:
```bash
# Check scheduler status
ps aux | grep airflow-scheduler

# Restart scheduler
airflow scheduler -D

# Check worker logs
tail -f /var/log/airflow/scheduler.log
```

### Issue: "Connection refused" to External Service

**Cause**: Missing or incorrect connection definition.

**Solution**:
```bash
# Add connection via CLI
airflow connections add 'warehouse_conn' \
    --conn-uri 'postgresql+psycopg2://user:pass@localhost:5432/warehouse'

# Or via Airflow UI: Admin > Connections
```

### Issue: XCom Not Passing Data Between Tasks

**Cause**: Task dependency not properly defined or serialization issue.

**Solution**:
```python
# Ensure proper XCom usage
# In producer task:
ti.xcom_push(key='result', value=df.to_json())

# In consumer task:
data = ti.xcom_pull(key='result', task_ids='transform_data')
df = pd.read_json(data)
```

### Issue: DAG Not Appearing in UI

**Cause**: DAG file in wrong location or syntax error.

**Solution**:
```bash
# Check DAG folder
ls -la $AIRFLOW_HOME/dags/

# Validate DAG syntax
python -m py_compile /path/to/your/dag.py

# Refresh DAG in UI
airflow dags list
```

### Issue: High Memory Usage

**Cause**: Loading large datasets entirely into memory.

**Solution**:
```python
# Use chunked processing
def transform_large_file(**context):
    chunk_size = 10000
    for chunk in pd.read_csv('/large/file.csv', chunksize=chunk_size):
        processed = chunk.apply(processing_logic)
        processed.to_sql('output_table', if_exists='append')
```

## Best Practices

### 1. Idempotency

Always make tasks idempotent - running multiple times produces the same result:

```python
def load_data(**context):
    execution_date = context['ds']
    
    # Use UPSERT to handle re-runs
    sql = """
        INSERT INTO target_table (id, data, loaded_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            data = EXCLUDED.data,
            loaded_at = EXCLUDED.loaded_at
    """
```

### 2. Error Handling

```python
from airflow.exceptions import AirflowFailException, AirflowSkipException

def critical_task(**context):
    try:
        result = risky_operation()
    except TemporaryError as e:
        raise AirflowSkipException(f"Temporary issue: {e}")
    except PermanentError as e:
        raise AirflowFailException(f"Critical failure: {e}")
```

### 3. Resource Management

```python
from airflow.models import BaseOperator
from airflow.utils.task_group import TaskGroup

with TaskGroup('data_processing') as tg:
    # Limit concurrent tasks
    for partition in partitions:
        PythonOperator(
            task_id=f'process_{partition}',
            python_callable=process_partition,
            pool='limited_pool',  # Restrict parallelism
        )
```

### 4. Monitoring & Alerts

```python
from airflow.operators.email import EmailOperator

notify_on_failure = EmailOperator(
    task_id='notify_failure',
    to='data-team@example.com',
    subject='DAG {{ dag.dag_id }} Failed',
    html_content="""
        <h3>Pipeline Failure Alert</h3>
        <p>DAG: {{ dag.dag_id }}</p>
        <p>Failed Task: {{ task.instance_id }}</p>
        <p>Log: {{ ti.log_url }}</p>
    """,
    trigger_rule='all_failed',
)
```

## Configuration Example

```ini
# airflow.cfg - Production settings

[core]
dags_folder = /opt/airflow/dags
base_log_folder = /var/log/airflow
executor = KubernetesExecutor

[scheduler]
num_runs = 1
min_file_process_interval = 30
dag_dir_list_interval = 300

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow
sql_alchemy_pool_size = 10

[logging]
base_log_folder = /var/log/airflow
remote_logging = true
s3_log_folder = s3://airflow-logs/
```

## Integration Patterns

### Snowflake Integration

```python
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

run_snowflake_query = SnowflakeOperator(
    task_id='execute_transformation',
    snowflake_conn_id='snowflake_conn',
    sql="""
        INSERT INTO analytics.daily_metrics (date, metric, value)
        SELECT 
            DATE_TRUNC('day', TIMESTAMP_SECONDS(CAST({{ ti.xcom_pull(task_ids='extract', key='timestamp') }} AS INT))),
            'revenue',
            SUM(amount)
        FROM raw.sales
        WHERE DATE(loaded_at) = '{{ ds }}'
        GROUP BY 1
    """,
)
```

### S3 Data Pipeline

```python
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator, S3DeleteObjectsOperator

fetch_data = S3ListOperator(
    task_id='list_s3_files',
    bucket='raw-data-bucket',
    prefix='daily/{{ ds }}/',
    aws_conn_id='aws_default',
)

process_file = S3ToS3Operator(
    task_id='process_and_upload',
    source_bucket='raw-data-bucket',
    source_key="{{ ti.xcom_pull(task_ids='list_s3_files')[0] }}",
    dest_bucket='processed-data-bucket',
    dest_key="processed/{{ ds }}/data.parquet",
    transform_script='/scripts/transform.py',
)
```

---

**Skill Version**: 1.0.0  
**Last Updated**: 2024-01-15  
**Compatible Airflow**: 2.7+, 2.8+  
```