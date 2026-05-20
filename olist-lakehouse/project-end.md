Pushing Airflow to the absolute end is a highly strategic move for this timeline. It completely eliminates the initial configuration overhead of setting up a local metadata database and scheduler right now, allowing you to focus **100% of your energy on code, data logic, and Iceberg mechanics.**

By decoupling Airflow, we will use simple shell scripts (`.sh`) or direct terminal executions to link your steps together for the first few days.

Here is your updated, hyper-focused 9-day schedule with Airflow shifted to Day 9.

---

## The Reprioritized 9-Day Schedule

### Phase 1: Parameterized Scala Jobs & Dataproc Core (Days 1–3)

*Goal: Write a single, generic Scala runner for all 8 remaining datasets and verify execution in the cloud using vanilla Parquet.*

* **Day 1: Meta-Driven Scala Refactoring**
* Instead of writing 8 separate processing scripts, refactor your `spark-jobs` repo to use a single, generic Scala object that reads configurations dynamically (e.g., passing arguments for `inputPath`, `outputPath`, and a list of timestamp columns to cast).
* Map out your transformation parameters for the remaining 8 Olist datasets (casting string timestamps for `orders`, `reviews`, etc.).
* Test your generic job locally inside WSL for 1 or 2 tables to verify the logic.


* **Day 2: Fat JAR Packaging & Dataproc Cluster Setup**
* Add `sbt-assembly` to your project and run `sbt assembly` to compile your code into a single deployment JAR.
* Spin up a lightweight Google Cloud Dataproc cluster via the GCP Console.
* Upload your JAR to GCS and run your ingestion script (`ingest.py`) manually to make sure all raw data is in your Bronze bucket.


* **Day 3: Dataproc Processing Loop (Vanilla Parquet)**
* Run a simple bash loop or manually execute `gcloud dataproc jobs submit spark` CLI commands for each of your 9 tables.
* Verify that your fat JAR processes raw Bronze Parquet files and drops clean, typed Parquet files into your Silver bucket.



---

### Phase 2: Iceberg SCD & Schema Evolution Mastery (Days 4–5)

*Goal: Learn Iceberg's native SQL mechanics, update your code, and map to BigQuery.*

* **Day 4: Iceberg Mechanics & Code Conversion**
* **The Concept:** Learn how Apache Iceberg completely eliminates manual outer-join code. You will switch from Spark DataFrame writes to Spark SQL blocks using `MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET *`.
* **Schema Evolution:** Learn how Iceberg tracks column IDs inside metadata snapshots, allowing you to drop or add columns instantly via `ALTER TABLE` without rewriting data files.
* **The Code:** Update your Scala Spark session config to include Iceberg extensions, and swap your write logic to use `format("iceberg")`.


* **Day 5: Dataproc Iceberg Upgrade & BigLake Tables**
* Re-create your Dataproc cluster, passing the initialization properties to activate the Iceberg runtime jars and the BigLake Metastore.
* Run your processing loop to write all 9 tables as native Iceberg formats in GCS.
* Create a BigLake connection in BigQuery and register those Iceberg directories as external tables so you can query them instantly with pure SQL.



---

### Phase 3: dbt Modeling, Looker BI, & Airflow Clean-up (Days 6–9)

*Goal: Build the semantic warehouse, visualize metrics, and automate the completed stack at the finish line.*

* **Day 6: dbt Staging Layer Setup**
* Initialize your dbt project and connect it directly to your BigQuery dataset containing the 9 BigLake Iceberg tables.
* Build your dbt staging layers (`stg_orders.sql`, `stg_customers.sql`, etc.) to standardize aliases.


* **Day 7: Gold Analytical Modeling**
* Write your core dbt analytical SQL models to join your Silver tables into business datasets: monthly GMV by category, shipping velocity, and seller performance metrics.
* Execute `dbt run` to materialize these as clean tables in BigQuery.


* **Day 8: Looker Studio Dashboarding**
* Connect Looker Studio directly to your dbt-generated Gold tables in BigQuery.
* Design an executive data engineering dashboard tracking order volume, regional distributions, and revenue performance.


* **Day 9 (May 31st): Airflow Orchestration & End-to-End Audit**
* Now that every single component works perfectly, write one simple Airflow DAG inside your WSL environment.
* Use a `PythonOperator` to trigger `ingest.py`, a looped `DataprocSubmitJobOperator` to run your Iceberg Scala JAR, and a `BashOperator` to execute `dbt run`.
* Clear your buckets, trigger the DAG, and watch the whole data stream run automatically from landing to dashboard.



---

### Immediate Next Step for Day 1

To build the parameterized Scala runner today, we need a way to pass configuration maps. A clean, senior-level approach is to pass a table name as an execution argument, and use a Scala match statement to assign the specific column properties.

Do your remaining 8 datasets have any highly custom field adjustments, or is it strictly transforming date strings into proper Spark `TimestampType` values?