# 🧠 Customer 360 Data Platform

An end-to-end data engineering pipeline to build a 360° view of customers using Apache Airflow, Jupyter notebooks, PySpark, and Snowflake — all orchestrated in a Docker environment.

---

## 📌 Key Features

- Automated ETL using **Apache Airflow + Papermill**
- Notebook-based modular tasks: **Extract**, **Transform**, **Load**
- Built with **Docker**, **PySpark**, and **Snowflake**
- CI/CD with **GitHub Actions**
- Easily extendable for **data quality checks**, **dashboards**, or **alerts**

---

## 🛠️ Tech Stack

| Layer        | Tool/Tech                     |
|-------------|-------------------------------|
| Orchestration | Apache Airflow               |
| Notebooks     | Jupyter + Papermill          |
| Data Engine   | PySpark                      |
| Cloud DB      | Snowflake                    |
| Scheduler     | Airflow + cron (optional)    |
| Containerization | Docker, Docker Compose    |
| CI/CD         | GitHub Actions               |

---

## 🧱 Folder Structure

```
customer360/
│
├── airflow/                # Airflow environment
│   ├── dags/               # DAG scripts
│   ├── logs/               # Airflow logs
│   └── airflow.cfg         # Config (auto-generated)
│
├── work/
│   └── etl/                # All Jupyter notebooks
│       ├── extract.ipynb
│       ├── transform.ipynb
│       └── load_to_snowflake.ipynb
│
├── data/                   # Source data (CSV, logs)
│   ├── customers.csv
│   ├── tickets.csv
│   └── logs/
│
├── staging/                # Intermediate parquet files
├── docker-compose.yml
└── README.md
```

---

## 🔁 ETL Workflow

```mermaid
flowchart TD
    A[Start DAG: customer360_etl] --> B[Extract Task]
    B --> B1[extract_crm() & extract_tickets() & extract_logs()]
    B1 --> C[Transform Task]
    C --> C1[PySpark clean & join]
    C1 --> D[Load Task]
    D --> D1[load_to_snowflake()]
    D1 --> E[End DAG]
```

---

## 🚀 Quickstart

### 1. Clone and Run

```bash
git clone https://github.com/your-username/customer360-etl
cd customer360-etl
docker-compose up -d
```

### 2. Open JupyterLab

Visit: `http://localhost:8888` (token auto-generated in logs)

### 3. Start Airflow

```bash
docker exec -it <airflow_container_name> airflow standalone
```

Or inside container:

```bash
airflow scheduler
airflow webserver
```

---

## 🧪 Trigger DAG

Manually from CLI:

```bash
airflow dags trigger customer360_etl
```

Or schedule via UI (disable `schedule=None` in DAG if needed).

---

## 📝 Notebooks

- **extract.ipynb**: loads CRM, support ticket, and log data
- **transform.ipynb**: joins, deduplicates, and enriches via PySpark
- **load_to_snowflake.ipynb**: uploads the results to Snowflake

Make sure these are runnable independently with the required paths!

---

## ⚠️ Troubleshooting Tips

- Create `staging/` directory before DAG run:
  ```bash
  mkdir -p /home/jovyan/work/staging
  ```

- Airflow not finding your DAG?
  ```bash
  airflow dags list
  airflow dags list-import-errors
  ```

- Use absolute paths in your notebook for CSVs and staging files.

---

## ⚙️ GitHub Actions (CI/CD)

> CI/CD ensures that your DAGs, notebooks, and Docker setup are valid and deployable.

**Workflows (in `.github/workflows/`)**
- Lint Python code
- Validate Airflow DAGs
- Optional: push image to DockerHub or deploy to cloud

---

## ✅ What's Done

- [x] Local Dockerized Airflow
- [x] Jupyter notebook ETL jobs
- [x] DAG working with Papermill
- [x] Debugged and tested with real data
- [x] Ready for GitHub Actions setup

---

## 🧩 Optional Enhancements

- Add **Great Expectations** or **dbt tests**
- Use **S3** or **GCS** instead of local staging
- Connect to **Snowflake securely via Secrets Manager**
- Add **alerts** or Slack notifications on failure

---

## 🧠 Author

Built by [Your Name](https://github.com/your-username)  
Feel free to fork or contribute 🤝

---