# Diebeties project on pyspark
This is a pyspark project that simulates end-to-end life cycle of a pyspark application. In this application I am going to deal with the development + automated testing + deployment of the pyspark application on databricks 
## Project Structure:  
```bash
your_project/
├── datasets/
│   ├── Bronze/                # raw CSVs used for local dev
│   ├── Silver/                # (local outputs for testing)
│   └── Gold/
├── src/
│   └── your_app/
│       ├── __init__.py
│       ├── cli.py             # entrypoint for local runs
│       ├── spark_init.py      # create SparkSession for local/dev
│       ├── ingest/            # ingestion-related utilities (optional)
│       │   └── ingest.py
│       ├── transforms/        # transformations (unit-testable)
│       │   └── patients.py
│       └── jobs/
│           └── process_patients.py  # job orchestration
├── tests/
│   ├── conftest.py           # pytest fixtures (SparkSession)
│   ├── test_patients.py
│   └── test_integration.py   # optional
├── conf/
│   ├── spark.conf            # spark configs for local/dev
│   └── databricks.yml        # optional databricks bundle metadata
├── jobs/                     # Databricks job yamls (optional)
│   └── process_patients_job.yaml
├── setup.py                  # package build
├── pyproject.toml
├── requirements.txt
├── .github/
│   └── workflows/
│       ├── test-and-deploy-dev.yml
│       ├── test-and-deploy-qa.yml
│       └── test-and-deploy-prod.yml
└── README.md
```
