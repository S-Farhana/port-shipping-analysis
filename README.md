# port-shipping-analysis
## Overview
This project implements a distributed data processing pipeline using Hadoop MapReduce to analyze global port and shipping data. It processes large-scale datasets to extract insights on cargo flow, vessel delays, and route efficiency.
The entire system runs on a Dockerized Hadoop cluster, making it easy to deploy and reproduce.

## Objectives
Understand HDFS architecture (NameNode, DataNode, YARN)
Implement parallel data processing using MapReduce
Use Combiner, Partitioner, and Job Chaining
Generate insightful visualizations using Python

## Dataset
- Source: Kaggle – Daily Port Activity Data
- Size: ~3.4 million rows, 30 columns
- Additional Data: Synthetic vessel dataset (5,000 records)

## Key Fields
- portname – Port name
- country – Country
- export, import – Cargo values
- portcalls_cargo – Traffic volume
- region – (Added) Asia / Europe / Americas
- dwell_hours – Vessel delay

## Tech Stack
- Hadoop 3.3.6 (HDFS + YARN)
- Docker (cluster setup)
- Java (MapReduce)
- Python (data prep + visualization)
- Libraries: pandas, matplotlib, seaborn, folium

## Setup & Execution
1. Clone Repository
```bash
git clone https://github.com/S-Farhana/port-shipping-analysis.git
cd port-shipping-analysis
```
2.Install Dependencies
```bash
pip install kagglehub pandas numpy faker matplotlib seaborn folium
```
3. Prepare Data
```bash
python scripts/download_data.py
python scripts/clean_data.py
python scripts/generate_vessel_schedule.py
```
4. Start Hadoop Cluster
```
docker-compose up -d
```
5. Load Data into HDFS
```
docker exec -it port-shipping-analysis-namenode-1 hdfs dfs -mkdir -p /port/input

docker cp data/processed/port_data_clean.csv port-shipping-analysis-namenode-1:/tmp/
docker cp data/processed/vessel_schedule.csv port-shipping-analysis-namenode-1:/tmp/

docker exec -it port-shipping-analysis-namenode-1 hdfs dfs -put /tmp/port_data_clean.csv /port/input/
docker exec -it port-shipping-analysis-namenode-1 hdfs dfs -put /tmp/vessel_schedule.csv /port/input/
```

## MapReduce Jobs
### Job 1 — Cargo Volume
- Calculates total cargo handled per port
- Uses Combiner for optimization
### Job 2 — Vessel Delay
Computes average vessel dwell time per port
### Job 3 — Route Efficiency
- Efficiency = cargo_tons / dwell_hours
- Output grouped by port and cargo type
### Job 4 — Chained Jobs + Partitioner
- Chains multiple jobs using JobControl
- Splits output into:
- Asia
- Europe
- Americas

## Author
- Anandika M
- S. Farhana
- Big Data Mini Project — 2026

