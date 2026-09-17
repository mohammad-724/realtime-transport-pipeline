readme url: https://mohammad-724.github.io/realtime-transport-pipeline

# Real-Time Public Transport Data Pipeline

A simple, professional Data Engineering project that simulates live public-transport telemetry and processes it through:

**Python → Kafka → Spark Structured Streaming → MySQL**

This project is designed for local Windows development **without Docker**.

## What the project does

A Python producer continuously creates simulated bus events containing:

- vehicle ID
- route ID
- timestamp
- latitude / longitude
- speed
- passenger count
- capacity
- operational status

Kafka receives the events in the `transport_events` topic.

Spark Structured Streaming reads the events, validates and transforms them, then writes the processed operational data to MySQL.

## Architecture

```text
Python Producer
      |
      v
Apache Kafka
  transport_events
      |
      v
Spark Structured Streaming
  - parse JSON
  - clean data
  - calculate delay
  - classify speed
  - detect overcrowding
      |
      v
MySQL
  transport_operations
```

## Prerequisites

Install these before running the project:

- Python 3.10+
- Java 17+ (Java 21 is also suitable)
- Apache Kafka 4.3.1
- MySQL 8.x
- VS Code
- Git (optional)

This repository assumes Kafka is installed directly on Windows, not through Docker.

## 1. Configure MySQL

Run `sql/database.sql` in MySQL Workbench.

Then copy:

```text
.env.example
```

to:

```text
.env
```

Edit the MySQL values in `.env`.

Example:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transport_events

MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=transport_db
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
```

Never commit `.env`.

## 2. Create the Python environment

From the project root:

```powershell
python -m venv venv
.\venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 3. Configure Kafka

The helper script assumes Kafka is installed at:

```text
C:\kafka\kafka_2.13-4.3.1
```

If you installed Kafka somewhere else, edit:

```text
scripts\config.ps1
```

and change `KAFKA_HOME`.

Kafka 4.x uses KRaft and does not require ZooKeeper.

For the first run, use the official Kafka local setup process to generate a cluster ID, format the storage, and start the broker.

## 4. Create the Kafka topic

With Kafka running:

```powershell
.\scripts\create_topic.ps1
```

The topic is:

```text
transport_events
```

## 5. Start the Python producer

Open a new PowerShell:

```powershell
.\venv\Scripts\activate
python producer\transport_producer.py
```

You should see events being sent every two seconds.

## 6. Start Spark Structured Streaming

Open another PowerShell:

```powershell
.\venv\Scripts\activate
.\scripts\run_spark.ps1
```

The script supplies the Spark Kafka connector automatically.

## 7. Check MySQL

Run:

```sql
USE transport_db;

SELECT *
FROM transport_operations
ORDER BY id DESC
LIMIT 20;
```

Rows should appear continuously while the producer and Spark job are running.

## Useful SQL

See delayed events:

```sql
SELECT vehicle_id, route_id, speed, delay_minutes, event_time
FROM transport_operations
WHERE delay_minutes > 5
ORDER BY event_time DESC;
```

See overcrowded vehicles:

```sql
SELECT vehicle_id, route_id, passengers, capacity, event_time
FROM transport_operations
WHERE overcrowded = TRUE
ORDER BY event_time DESC;
```

Average speed by route:

```sql
SELECT
    route_id,
    ROUND(AVG(speed), 2) AS average_speed
FROM transport_operations
GROUP BY route_id
ORDER BY route_id;
```

Route operational summary:

```sql
SELECT
    route_id,
    ROUND(AVG(speed), 2) AS average_speed,
    ROUND(AVG(passengers), 2) AS average_passengers,
    SUM(CASE WHEN delay_minutes > 5 THEN 1 ELSE 0 END) AS delayed_events,
    SUM(CASE WHEN overcrowded = TRUE THEN 1 ELSE 0 END) AS overcrowded_events
FROM transport_operations
GROUP BY route_id
ORDER BY route_id;
```

## Startup order

1. Start Kafka.
2. Create the topic.
3. Start Python producer.
4. Start Spark.
5. Query MySQL.

Stop producer or Spark with `Ctrl+C`.

## Troubleshooting

### Kafka folder not found

Edit:

```text
scripts\config.ps1
```

and set the correct `KAFKA_HOME`.

### Spark cannot find Kafka source

Use:

```powershell
.\scripts\run_spark.ps1
```

instead of directly running `transport_stream.py`. The script adds the required Kafka connector with `--packages`.

### MySQL connection failed

Check the `.env` values and confirm MySQL Server is running.

### Port 9092 is already in use

Check whether another Kafka broker is already running.

## Final scope

**Input:** simulated real-time public transport events

**Streaming:** Apache Kafka

**Processing:** Spark Structured Streaming

**Storage:** MySQL

**Output:** cleaned and transformed operational transport datasets
