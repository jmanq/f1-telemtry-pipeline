This project builds a full end-to-end data pipeline for analyzing Formula 1 race sessions using FastF1, Databricks, and a Streamlit web app.
The pipeline ingests raw telemetry + lap data, processes it into analytical gold tables, and makes it explorable through an interactive app.

📸 Screenshots of the Streamlit app are included in the repository.


Features
 1. Data Ingestion (Databricks Notebook)

Pulls telemetry and lap data from FastF1 for any race session (season, event, session).

Writes data into bronze Delta tables (bronze_laps and bronze_telemetry).

Automatically stores:

lap times

stint data

tire compounds

best-lap telemetry for each stint

event metadata

 2. Transformation Pipeline (Databricks Notebook)

Creates full silver and gold datasets:

Lap, sector, and stint-level aggregates

Reliability scoring based on:

large gap rate

data completeness

drift stability

Human-readable SQL views for:

stint performance

stint-level telemetry reliability

sector averages

best-in-compound deltas

This process makes the data clean, structured, and ready for analytics.

 3. Interactive Streamlit App

Connects to Databricks via SQL Warehouse using the databricks-sql-connector.

Lets you explore:

driver stints

lap time deltas

reliability scores

stint summaries

Automatically loads sessions that have been previously ingested (no need to rerun ingestion).
