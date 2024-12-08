### Day 1

**Apache Spark Driver Execution Flow:**

- **The Plan**: The overall strategy that needs to be executed.
- **The Driver**: The central coordinator that reads the plan and orchestrates execution.
- **The Executors**: The workers that perform the tasks.

#### **Driver Responsibilities**
- Decides when to start executing the job.
- Determines how to join datasets.
- Specifies the level of parallelism required for each step.
  
**Key Parameters:**
- `spark.driver.memory`
- `spark.driver.memoryOverheadFactor`

#### **Executor Responsibilities**
The driver passes the execution plan to the executors, which then carry out the tasks.
  
**Key Parameters:**
- `spark.executor.memory`
- `spark.executor.cores`
- `spark.executor.memoryOverheadFactor`

#### **Types of Joins in Spark**

- **Shuffle Sort-Merge Join**:
  - Default join strategy since Spark 2.3.
  - Effective when both sides of the join are large.

- **Broadcast Hash Join**:
  - Ideal when the left side of the join is small.
  - `spark.autoBroadcastJoinThreshold` is the related configuration.
  - A join strategy that avoids shuffle operations.

- **Bucket Join**:
  - A join strategy that avoids shuffle operations, useful for predefined bucketed datasets.

#### **Handling Skew**

- Set `spark.sql.adaptive.enabled = True` to improve resilience to skew. This option may slow down the job, as it requires the computation of statistics.
- Filter out IDs causing skew to further optimize performance.

#### **How Spark Reads Data**

- From a **data lake** (e.g., Delta Lake, Apache Iceberg, Hive).
- From **RDBMS** (e.g., Postgres, Oracle).
- From an **API** (be cautious, as this operation occurs on the driver and may lead to out-of-memory issues).
- From **flat files** (e.g., CSV, JSON).

#### **Spark Output Dataset**
- Should always be partitioned by the date.
- The date should correspond to the execution date of the pipeline.
