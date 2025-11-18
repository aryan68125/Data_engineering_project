# Diebeties project on pyspark
This is a pyspark project that simulates end-to-end life cycle of a pyspark application. In this application I am going to deal with the development + automated testing + deployment of the pyspark application on databricks 
## Project environment 
I have exported my project's environment using conda like this 
```bash
conda env export > environment.yml
```
my exported conda environment
```bash
name: databricks
channels:
  - defaults
dependencies:
  - _libgcc_mutex=0.1=main
  - _openmp_mutex=5.1=1_gnu
  - bzip2=1.0.8=h5eee18b_6
  - ca-certificates=2025.11.4=h06a4308_0
  - expat=2.7.3=h3385a95_0
  - ld_impl_linux-64=2.44=h153f514_2
  - libffi=3.4.4=h6a678d5_1
  - libgcc=15.2.0=h69a1729_7
  - libgcc-ng=15.2.0=h166f726_7
  - libgomp=15.2.0=h4751f2c_7
  - libnsl=2.0.0=h5eee18b_0
  - libstdcxx=15.2.0=h39759b7_7
  - libstdcxx-ng=15.2.0=hc03a8fd_7
  - libuuid=1.41.5=h5eee18b_0
  - libxcb=1.17.0=h9b100fa_0
  - libzlib=1.3.1=hb25bd0a_0
  - ncurses=6.5=h7934f7d_0
  - openssl=3.0.18=hd6dcaed_0
  - pip=25.2=pyhc872135_1
  - pthread-stubs=0.3=h0ce48e5_1
  - python=3.11.14=h6fa692b_0
  - readline=8.3=hc2a1206_0
  - setuptools=80.9.0=py311h06a4308_0
  - sqlite=3.51.0=h2a70700_0
  - tk=8.6.15=h54e0aa7_0
  - wheel=0.45.1=py311h06a4308_0
  - xorg-libx11=1.8.12=h9b100fa_1
  - xorg-libxau=1.0.12=h9b100fa_0
  - xorg-libxdmcp=1.1.5=h9b100fa_0
  - xorg-xorgproto=2024.1=h5eee18b_1
  - xz=5.6.4=h5eee18b_1
  - zlib=1.3.1=hb25bd0a_0
  - pip:
      - cachetools==6.2.1
      - certifi==2025.10.5
      - charset-normalizer==3.4.4
      - click==8.3.0
      - databricks-cli==0.18.0
      - databricks-connect==16.1.7
      - databricks-sdk==0.73.0
      - google-auth==2.43.0
      - googleapis-common-protos==1.72.0
      - grpcio==1.76.0
      - grpcio-status==1.76.0
      - idna==3.11
      - numpy==1.26.4
      - oauthlib==3.3.1
      - packaging==25.0
      - pandas==2.3.3
      - protobuf==6.33.0
      - py4j==0.10.9.7
      - pyarrow==22.0.0
      - pyasn1==0.6.1
      - pyasn1-modules==0.4.2
      - pyjwt==2.10.1
      - python-dateutil==2.9.0.post0
      - pytz==2025.2
      - requests==2.32.5
      - rsa==4.9.1
      - six==1.17.0
      - tabulate==0.9.0
      - typing-extensions==4.15.0
      - tzdata==2025.2
      - urllib3==2.5.0
prefix: /home/aditya/miniconda3/envs/databricks
```
You can use this exported environment to replicate the environemnt that your used in your development using this command 
```bash
conda env create -f environment.yml
```
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
