# spark-lab: learning Apache Spark from the engine up

A hands-on lab where I learn Spark by running it, measuring it and breaking it on purpose:
from a hello-world on my laptop, to a standalone cluster, then a remote Linux server and Kubernetes.
Every lab answers one question about **how Spark works inside**, with evidence from logs, `jps`,
exit codes and the Spark UI rather than theory alone.

## What I measured so far

| Lab | Question | What I found |
|---|---|---|
| [`a1_hello.py`](labs/a1_hello.py) | What is the smallest Spark program? | A `SparkSession`, one DataFrame, one action, and the Spark UI on port 4040 |
| [`a2_lazy.py`](labs/a2_lazy.py) | When does Spark actually do work? | Transformations only build a plan; nothing runs until an action. Errors in a transformation appear only at the action |
| [`a4_partitions.py`](labs/a4_partitions.py) | How does Spark split a file into tasks? | A `.gz` file is not splittable: **1 task**. The same data as plain JSON: **12 tasks** (one per core). `repartition` and SQL hints change it |
| [`taxi_explore.py`](labs/taxi_explore.py), [`jobs/taxi_clean.py`](jobs/taxi_clean.py) | A real cleaning job on 12 months of NYC taxi data | January 2024: 2,964,624 raw rows → 2,869,528 clean (3.21% removed). The job refuses to write if it would remove more than 10% |
| [`b1_two_sides.py`](labs/b1_two_sides.py) | Is PySpark "Spark"? | One PySpark job is **two processes**: Python (client) and a JVM (the engine), joined by Py4J. The PySpark package is 484 MB, 456 MB of it are **277 Java jars**. With `spark-submit` Java is the parent of Python; with `python file.py` it is the other way round |
| [`c1_cluster.py`](labs/c1_cluster.py) | What do master, worker, driver and executor really do? | Standalone cluster on one machine: 1 master + 2 workers. The master only allocates resources; executors register **directly with the driver**, which schedules the tasks |

## Breaking things on purpose

| Break | What happened | Why |
|---|---|---|
| Truncated `.gz` input file (download cut off) | Whole job fails after task retries | A gzip file is read by one task to its end; a cut file has no valid end. Now caught beforehand with `gzip -t` |

Next breaks to run: missing `JAVA_HOME`, killing an executor mid-job, executors larger than any worker, a worker dying.

## Roadmap

- [x] Spark on one machine: sessions, laziness, partitions, shuffles and stages, schemas, `spark-submit`
- [x] PySpark vs the JVM: two processes, the jars, who starts whom
- [ ] A Java job with `spark-submit --class`; the cost of Python UDFs
- [x] Standalone cluster: master, workers, driver, executors
- [ ] Cluster failures: executor killed, resources too small, worker dead
- [ ] Remote Linux server (AWS EC2): `ssh`, reading real logs, SSH tunnel to the UI
- [ ] Executor memory model in depth
- [ ] Spark on Kubernetes: containers, `kind`, MinIO (S3), `spark-submit` to K8s, Spark Operator, logs in Grafana/Loki
- [ ] Break-and-fix: joins and skew inside the engine, K8s failures

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install pyspark==4.2.0        # needs Java 17
python labs/a1_hello.py           # Spark UI at http://localhost:4040 while it runs
```

Data is downloaded by scripts (for example [`scripts/download_taxi.sh`](scripts/download_taxi.sh)) and never committed.

---

**Gulnar Albushova**, UC Berkeley Master of Information and Data Science (MIDS)
· [LinkedIn](https://www.linkedin.com/in/gulnar-albushova-253252140/) · [GitHub](https://github.com/Gunar-Oguz)
