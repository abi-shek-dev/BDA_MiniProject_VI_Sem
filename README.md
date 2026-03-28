# 🛒 Real-Time E-Commerce Sentiment Analyzer

A complete, end-to-end Big Data pipeline that ingests streaming e-commerce reviews, mathematically processes customer sentiment in real-time, stores the aggregated data, and visualizes the insights on an interactive live dashboard.

## 🏗️ Architecture & Tech Stack
* **Data Ingestion:** Apache Kafka (running via Docker)
* **Stream Processing:** Apache Spark (PySpark Structured Streaming)
* **Database:** MongoDB (NoSQL Document Store)
* **Frontend Dashboard:** Streamlit & Plotly
* **Language:** Python 3.13

---

## ⚙️ Prerequisites

Before running this project, ensure you have the following installed on your machine:
1. **Python 3.10+**
2. **MongoDB Community Server** (Running locally on default port `27017`)
3. **Java 8 or 11** (Required for Apache Spark)
4. **Hadoop Winutils** (For Windows users: placed in `C:\hadoop\bin`)
5. **Docker Desktop** (For running Kafka and Zookeeper)

---

## 🐳 Docker Setup Manual (For Kafka)

Apache Kafka requires a complex cluster environment to run. To make this easy, we use Docker to run Kafka inside isolated containers.

### Step 1: Install Docker Desktop
Download Docker Desktop and install it. Enable WSL 2 backend if prompted.

### Step 2: Start the Docker Engine
Open Docker Desktop and wait until it shows "Engine Running".

### Step 3: Spin up the Kafka Cluster
Run:
```
docker-compose up -d
```

---

## 🚀 Project Setup & Execution

### 1. Install Python Dependencies
```
pip install -r requirements.txt
```

### 2. Download the Dataset
```
python get_data.py
```

### 3. Launch the Pipeline

**Terminal 1:**
```
python -m streamlit run src/3_streamlit_app.py
```

**Terminal 2:**
```
python src/2_spark_processor.py
```

**Terminal 3:**
```
python src/1_kafka_producer.py
```

---

## 📊 Dashboard Features

- Live KPI Metrics
- Top Products Chart
- Sentiment Distribution
- Live Data Feed

---

## 🛠️ Troubleshooting

- Ensure Hadoop files are in `C:\hadoop\bin`
- Use PySpark native functions
- Use unique keys in Streamlit plots
