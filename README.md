# DataFarm

DataFarm is an innovative framework for efficiently generating and labeling large query workloads. 
It follows a data-driven & white-box approach to learn from pre-existing small workload patterns, input data, and computational resources. 
Thus, DataFarm allows users to produce a large heterogeneous set of realistic jobs with their labels, which can be used by any ML-based data management component.

```F. Ventura, Z. Kaoudi, J. Quiané-Ruiz, and V. Markl. Expand your Training Limits! Generating and Labeling Jobs for ML-based Data Management. In SIGMOD, 2021.```

## Requirements

- SBT >= 1.3 
- Scala == 2.11
- Flink == 1.10.0
- Python == 3.6

Install all the python requirements specified in `requirements.txt`.  
N.B. DtaFarm has been tested on Linux and MacOS.

## Quick-start

1. Update `CONFIG.py`
2. Update `TableMetaData.py` (if needed)
3. Run `RunGenerator.py` 
4. Run `RunLabelForecaster.py`: To submit the jobs it is necessary that a Flink cluster is running. Please, be sure that the cluster is running and accessible.

## Configuration
To configure DataFarm you have to edit the `CONFIG.py`.

Please, provide the following configurations to start generating jobs:

- Provide the absolute path to the DataFarm
    ```Python
    PROJECT_PATH = "/absolute/path/to/DataFarm/project"
    ```

- Provide the absolute path to the folder containing your input data
    ```Python
    GENERATED_JOB_INPUT_DATA_PATH = "/absolute/path/to/input/data" 
    ```
    This folder will contain your input data.
    
- Provide the absolute path to the flink compiled source.
    ```Python
    FLINK_HOME = "/absolute/path/to/flink"
    ```
    N.B. The current version of DataFarm has been tested on Flink 1.10.0 built with scala 2.11. You can download Flink from [here](https://archive.apache.org/dist/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz).
    
We provide a sample _Input Workload_ in the project folder `data/input_workload_exec_plan`. 
You can include here any execution plan extracted from Flink jobs.

We also provide a sample TPC-H input data (about 1GB).
You can download sample TPC-H input data from [here](https://www.kaggle.com/fven7u/tpch-1gb).

The provided `TableMetaData.py` already contains the information necessary to run DataFarm with TPC-H data with scale factors 1GB, 5GB, 10GB, 50GB.

### Generator Configuration
DataFarm can be configured to generate datasets with different characteristics:

- `N_JOBS` defines the number of diverse Abstract Plans that will be generated.
- `N_VERSIONS` defined the number of versions that will be generated for each Abstract Plan.
- `JOB_SEED` makes the generation process replicable with a seed of your choice. With -1, each job's seed is derived from its plan index (also deterministic); otherwise it is derived from `JOB_SEED` and the plan index, so different plans still get different tables, filters and group-by fields.
- `DATA_MANAGER` specifies the database manager to be used: "TPCH" for TPC-H, "JOB" for the Join Order Benchmark on IMDB (Wayang only, see [Join Order Benchmark (JOB)](#join-order-benchmark-job)).
- `DATA_ID` specifies the id of the input data meta-data that has to be used by the system. The input data meta-data can be specified in `TableMetaData.py`.
- `EXPERIMENT_ID` defines the id of the experiment. It will be the name of the folder where the results of the generation process will be stored.


### Label Forecaster Configuration
The Label Forecaster can be configured with:

- `MAX_EARLY_STOP` defines the max number of early stops that will be computed before interrupting the labeling process.
- `EARLY_STOP_TH` defines the threshold for early stop. It has to be included in the range (0, 1.0).
- `MAX_ITER` defines the maximum number of iterations that will be performed before interrupting the active learning iterations.
- `INIT_JOBS` defines the number of jobs to sample and run before starting the Active Learning process.
- `RANDOM_SAMPLING` defines if the instances will be picked with weighted random sampling based on uncertainty.

## Table Meta-Data
The `TableMetaData` defines all the ER information regarding your db. 
All the information have to be provided through python dictionary.

You should specify the possible join relations that you want to consider while instantiating new jobs.
Also, you should specify, for each table, which are the fields that can be filtered, and grouped.
Finally, you should also specify which are the fields that contains dates.

Then, also the raw cardinalities of the tables under exam should be specified.

To have an example of `TableMetaData` configuration, please look at the `TableMetaData.py` file.

## Join Order Benchmark (JOB)
DataFarm can also generate Wayang jobs over the IMDB database of the Join Order Benchmark
([Leis et al., PVLDB 2015](https://www.vldb.org/pvldb/vol9/p204-leis.pdf)).

```Python
DATA_MANAGER = "JOB"
TARGET_PLATFORM = "Wayang"
```

**Data.** The jobs read the 21 IMDB CSV files of the original JOB dump (`title.csv`, `cast_info.csv`, ...) from
the `datapath` argument of the generated job. Each generated `JobXvY.scala` contains its own parser object
(`JobXvYParsers`), so no JOB-specific classes are needed on the Wayang side. Integer columns become `Long`
(NULL is `Long.MinValue`), text columns `String` (NULL is `null`).

**What gets generated.** Tables, columns and join edges follow the JOB schema
([gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark)). Joins use the equi-join
edges that occur in the 113 JOB queries, including the direct `movie_id`/`person_id` joins between fact tables.
Filters are taken from the JOB queries (e.g. `company_name.country_code = '[us]'`, `title.production_year > 2000`,
`keyword.keyword in (...)`); tables that JOB never filters directly (`aka_title`, `complete_cast`,
`movie_keyword`, `movie_link`) get `id % m = 0` predicates. All of this lives in
`instantiator/src/main/resources/job/job_catalog.json`; edit it to add or remove predicates.

**Input workload.** With `DATA_MANAGER = "JOB"`, the abstract plans are learned from the JOB queries instead of
TPC-H: `data/input_workload_exec_plan_job/` holds one plan per JOB query (113), built by
`generator_labeler/JOB_InputWorkload.py` from the query text in the style of a hand-written Flink DataSet job
(per table a source and, if filtered, a filter; a left-deep chain of joins; the `MIN(...)` aggregate as reduce; a sink).
These plans describe the operator graph only, they were not produced by running Flink. Plan depth and the number
of joins are learned from this workload (`INPUT_WORKLOAD` in `CONFIG.py`), so generated jobs have 1 to ~16 joins
like JOB, and tables are reused at most as often as in a single JOB query. To rebuild the plans:

```bash
git clone https://github.com/gregrahn/join-order-benchmark
python generator_labeler/JOB_InputWorkload.py join-order-benchmark data/input_workload_exec_plan_job
```

**Statistics.** Selectivities, distinct counts and table sizes depend on your IMDB snapshot. Only the values of
the small lookup tables are filled in; everything else is `null` and is recorded as `-1` in
`generated_jobs_info.json` until you measure it:

```bash
python generator_labeler/DatasetMetadata/JOB_MetaDataAnalyzer.py /path/to/imdb_csv_dir
```

This writes the measured values into the catalog (use `--output` to write elsewhere and point the generator to it
with the environment variable `DATAFARM_JOB_CATALOG`). The analyzer uses the same parsing and NULL semantics as the
generated jobs.

**Limitations.**
- The generator keeps only the right-hand side of each join, so a job is a chain of joins in which each join
  uses a column of the table joined last. Star-shaped JOB queries therefore become chains, mostly through the
  `movie_id` joins between fact tables.
- JOB queries contain only sources, filters, joins and an aggregate, so JOB-based jobs contain no map, group-by
  or sort operators.
- Joins over fact tables on `movie_id` are many-to-many (as in JOB) and can produce very large intermediate results
  when no selective filter precedes them.
- `JOB_SEED` set to a fixed value makes every version `j` of every plan start from the same table; use `-1` for
  more variety.

